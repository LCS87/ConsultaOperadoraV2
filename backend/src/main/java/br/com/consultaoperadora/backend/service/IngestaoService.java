package br.com.consultaoperadora.backend.service;

import br.com.consultaoperadora.model.EmpresaRegistro;
import br.com.consultaoperadora.model.Operadora;
import br.com.consultaoperadora.backend.entity.ArquivoOrigemEntity;
import br.com.consultaoperadora.backend.entity.DuplicidadeLogEntity;
import br.com.consultaoperadora.backend.entity.ProcessamentoJobEntity;
import br.com.consultaoperadora.backend.entity.StatusProcessamento;
import br.com.consultaoperadora.backend.repository.ArquivoOrigemRepository;
import br.com.consultaoperadora.backend.repository.DuplicidadeLogRepository;
import br.com.consultaoperadora.backend.repository.EmpresaRegistroRepository;
import br.com.consultaoperadora.backend.repository.ProcessamentoJobRepository;
import br.com.consultaoperadora.service.CnpjValidator;
import br.com.consultaoperadora.service.CsvDelimiterDetector;
import br.com.consultaoperadora.service.CsvReaderService;
import br.com.consultaoperadora.service.OperadoraClassifier;
import br.com.consultaoperadora.service.TelefoneNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Serviço de ingestão com persistência (Fase 4).
 *
 * Reaplica as MESMAS regras do core (CNPJ, normalização, classificação), porém
 * grava cada registro classificado no banco. A deduplicação deixa de ser um
 * HashSet em memória e passa a ter DUAS camadas persistentes:
 *   1. dentro do arquivo (Set local, paridade com o legado);
 *   2. entre execuções (consulta + UNIQUE em empresa_registro.cnpj) —
 *      reprocessar o mesmo arquivo não duplica CNPJs.
 * Todo descarte é auditado em {@code duplicidade_log}.
 */
@Service
public class IngestaoService {

    private static final Logger log = LoggerFactory.getLogger(IngestaoService.class);

    private final ArquivoOrigemRepository arquivos;
    private final ProcessamentoJobRepository jobs;
    private final EmpresaRegistroRepository registros;
    private final DuplicidadeLogRepository duplicidades;
    private final Set<String> ufsPermitidas;
    private final CsvReaderService reader = new CsvReaderService(StandardCharsets.ISO_8859_1);

    public IngestaoService(ArquivoOrigemRepository arquivos,
                           ProcessamentoJobRepository jobs,
                           EmpresaRegistroRepository registros,
                           DuplicidadeLogRepository duplicidades,
                           @org.springframework.beans.factory.annotation.Value(
                                   "${app.ufs-permitidas:AL,BA,CE,MA,PB,PE,PI,RN,SE}") String ufsCsv) {
        this.arquivos = arquivos;
        this.jobs = jobs;
        this.registros = registros;
        this.duplicidades = duplicidades;
        this.ufsPermitidas = java.util.Arrays.stream(ufsCsv.split("\\s*,\\s*"))
                .map(s -> s.toUpperCase(Locale.ROOT)).collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    /**
     * Processa um CSV já materializado em disco, do início ao fim, em uma única
     * transação (job + arquivo + registros + auditoria são atômicos).
     *
     * @throws IllegalArgumentException nome do arquivo sem UF permitida
     * @throws IOException              falha de leitura/parse do CSV
     */
    @org.springframework.transaction.annotation.Transactional
    public ProcessamentoJobEntity processar(java.nio.file.Path csvPath) throws IOException {
        return processar(csvPath.toFile());
    }

    @org.springframework.transaction.annotation.Transactional
    public ProcessamentoJobEntity processar(File csvFile) throws IOException {
        String nomeArquivo = csvFile.getName();
        String uf = extrairUf(nomeArquivo);
        if (uf == null) {
            throw new IllegalArgumentException(
                    "Nome de arquivo '" + nomeArquivo + "' não corresponde a nenhuma UF permitida "
                            + ufsPermitidas + " (padrões aceitos: uf_*.csv ou *_uf.csv).");
        }

        char delimiter;
        try {
            delimiter = CsvDelimiterDetector.detect(csvFile, StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ArquivoOrigemEntity arquivo = new ArquivoOrigemEntity();
        arquivo.setNomeArquivo(nomeArquivo);
        arquivo.setUf(uf);
        arquivo.setEncoding(StandardCharsets.ISO_8859_1.name());
        arquivo.setDelimitadorDetectado(String.valueOf(delimiter));
        arquivo.setTamanhoBytes(csvFile.length());
        arquivo.setHashSha256(sha256(csvFile));
        arquivo.setCriadoEm(Instant.now());
        arquivos.save(arquivo);

        ProcessamentoJobEntity job = new ProcessamentoJobEntity();
        job.setArquivo(arquivo);
        job.setStatus(StatusProcessamento.EM_EXECUCAO);
        job.setIniciadoEm(Instant.now());
        jobs.save(job); // IDENTITY -> id disponível imediatamente

        Set<String> vistosNoArquivo = new java.util.HashSet<>();
        Map<Operadora, Integer> contagem = new LinkedHashMap<>();
        long[] counters = {0, 0, 0}; // lidas, inseridos, invalidos

        try {
            reader.forEachLine(csvFile, delimiter, rowData -> {
                counters[0]++;
                EmpresaRegistro registro = EmpresaRegistro.fromRow(rowData);
                if (registro == null) {
                    return; // linha incompleta (paridade com legado)
                }
                if (!CnpjValidator.isValidoCompleto(registro.cnpj())) {
                    counters[2]++;
                    return; // API usa critério rígido (formato + DV), como na Fase 3
                }
                String cnpjDigitos = CnpjValidator.apenasDigitos(registro.cnpj());
                String motivoDuplicidade = null;
                if (vistosNoArquivo.contains(cnpjDigitos)) {
                    motivoDuplicidade = DuplicidadeLogEntity.MOTIVO_REPETIDO_NO_ARQUIVO;
                } else if (registros.existsByCnpjForUpdate(cnpjDigitos)) {
                    motivoDuplicidade = DuplicidadeLogEntity.MOTIVO_JA_EXISTENTE_BANCO;
                }
                vistosNoArquivo.add(cnpjDigitos);
                if (motivoDuplicidade != null) {
                    duplicidades.save(DuplicidadeLogEntity.de(
                            job.getId(), cnpjDigitos, nomeArquivo, motivoDuplicidade));
                    job.setDuplicadosDescartados(job.getDuplicadosDescartados() + 1);
                    return; // dedupe PERSISTENTE: CNPJ já conhecido não entra de novo
                }
                Operadora operadora = OperadoraClassifier.classify(registro.telefonesRaw());
                registros.save(EmpresaRegistro.of(registro.asColumns(), cnpjDigitos, operadora, uf,
                        TelefoneNormalizer.normalizeFirstPhoneNumber(registro.telefonesRaw()), job.getId()));
                contagem.merge(operadora, 1, Integer::sum);
                counters[1]++;
            });
        } catch (IOException e) {
            // falha de leitura/parse do CSV -> registra no job e propaga (contrato do metodo)
            job.setStatus(StatusProcessamento.FALHA);
            job.setMensagemErro(truncate(e.getMessage(), 500));
            job.setFinalizadoEm(Instant.now());
            job.setLinhasLidas(counters[0]);
            jobs.save(job);
            log.error("Job {} falhou ao ler {}", job.getId(), nomeArquivo, e);
            throw e;
        } catch (RuntimeException e) { // inclui UncheckedIOException (subclass)
            job.setStatus(StatusProcessamento.FALHA);
            job.setMensagemErro(truncate(e.getMessage(), 500));
            job.setFinalizadoEm(Instant.now());
            job.setLinhasLidas(counters[0]);
            jobs.save(job);
            log.error("Job {} falhou ao processar {}", job.getId(), nomeArquivo, e);
            throw e;
        }

        job.setLinhasLidas(counters[0]);
        job.setRegistrosInseridos((int) counters[1]);
        job.setCnpjInvalidosDescartados((int) counters[2]);
        job.setPorOperadora(formatarContagem(contagem));
        job.setStatus(StatusProcessamento.CONCLUIDO);
        job.setFinalizadoEm(Instant.now());
        jobs.save(job);

        log.info("Job {}: {} linhas, {} inseridos, {} duplicados, {} CNPJs inválidos ({})",
                job.getId(), counters[0], counters[1],
                job.getDuplicadosDescartados(), counters[2], contagem);
        return job;
    }

    /** Última visão agregada do banco para o dashboard (contagens reais). */
    public DashboardSnapshot dashboard() {
        List<Object[]> porOperadoraUf = registros.contagemPorOperadoraUf();
        return new DashboardSnapshot(
                registros.count(),
                jobs.count(),
                porOperadoraUf.stream()
                        .map(row -> new OperadoraUf(((Operadora) row[0]).name(), (String) row[1],
                                ((Number) row[2]).longValue()))
                        .toList());
    }

    public record OperadoraUf(String operadora, String uf, long quantidade) { }
    public record DashboardSnapshot(long totalRegistros, long totalJobs, List<OperadoraUf> itens) { }

    /** Mesma lógica de extração de UF do motor CLI/core (paridade). */
    String extrairUf(String fileName) {
        String lowerCaseName = fileName.toLowerCase(Locale.ROOT);
        for (String uf : ufsPermitidas) {
            String lowerUf = uf.toLowerCase(Locale.ROOT);
            if (lowerCaseName.contains(lowerUf + "_") || lowerCaseName.endsWith("_" + lowerUf + ".csv")) {
                return uf;
            }
        }
        return null;
    }

    private static String formatarContagem(Map<Operadora, Integer> contagem) {
        StringBuilder sb = new StringBuilder();
        contagem.forEach((op, qtd) -> {
            if (!sb.isEmpty()) {
                sb.append("; ");
            }
            sb.append(op.fileNameToken()).append('=').append(qtd);
        });
        return truncate(sb.toString(), 1000);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static String sha256(File file) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (var in = new java.io.FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int read;
                while ((read = in.read(buf)) > 0) {
                    md.update(buf, 0, read);
                }
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException | IOException e) {
            log.warn("Não foi possível calcular hash SHA-256 de {}", file.getName(), e);
            return null;
        }
    }
}
