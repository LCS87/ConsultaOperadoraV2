package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.model.EmpresaRegistro;
import br.com.consultaoperadora.model.Operadora;
import br.com.consultaoperadora.service.CnpjValidator;
import br.com.consultaoperadora.service.CsvDelimiterDetector;
import br.com.consultaoperadora.service.CsvReaderService;
import br.com.consultaoperadora.service.DeduplicationService;
import br.com.consultaoperadora.service.OperadoraClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Motor de classificação sob demanda (Fase 3 — API REST).
 *
 * Reaplica exatamente as mesmas regras do pipeline CLI (Fases 0-2), porém em
 * modo "on demand": recebe um arquivo CSV (ex.: upload via HTTP) e devolve o
 * resultado classificado em memória, sem escrever arquivos de saída.
 *
 * É intencionalmente uma classe POJO (sem dependência de Spring) para manter o
 * núcleo reutilizável e testável; a camada web apenas o envolve.
 */
public final class MotorClassificacaoService {

    private static final Logger log = LoggerFactory.getLogger(MotorClassificacaoService.class);

    /** Resultado imutável da classificação de um arquivo. */
    public record Resultado(
            String fileName,
            String uf,
            long linhasLidas,
            int registrosUnicos,
            Map<String, Long> contagemPorOperadora,          // token -> qtd (ex.: CLARO -> 12)
            Map<String, List<String[]>> detalhePorOperadora) { // token -> colunas (6)
    }

    private final List<String> ufsPermitidas;
    private final CsvReaderService readerService = new CsvReaderService(StandardCharsets.ISO_8859_1);

    public MotorClassificacaoService(List<String> ufsPermitidas) {
        this.ufsPermitidas = List.copyOf(ufsPermitidas);
    }

    /**
     * Classifica um arquivo CSV presente em disco.
     *
     * @throws IllegalArgumentException se o nome não corresponde a nenhuma UF permitida
     * @throws IOException              em falha de leitura/validação do CSV
     */
    public Resultado classificarArquivo(Path csvPath) throws IOException {
        return classificarArquivo(csvPath, true);
    }

    /**
     * @param validarDigitosVerificadores {@code true} (API REST): critério rígido da
     *        Receita (formato + DV). {@code false} (CLI/paridade): apenas 14 dígitos.
     */
    public Resultado classificarArquivo(Path csvPath, boolean validarDigitosVerificadores) throws IOException {
        String fileName = csvPath.getFileName().toString();
        String uf = extrairUf(fileName);
        if (uf == null) {
            throw new IllegalArgumentException(
                    "Nome de arquivo '" + fileName + "' não corresponde a nenhuma UF permitida "
                            + ufsPermitidas + " (padrões aceitos: uf_*.csv ou *_uf.csv).");
        }

        char delimiter = CsvDelimiterDetector.detect(csvPath.toFile(), StandardCharsets.ISO_8859_1);

        Map<Operadora, List<EmpresaRegistro>> buckets = new EnumMap<>(Operadora.class);
        for (Operadora o : Operadora.values()) {
            buckets.put(o, new ArrayList<>());
        }
        DeduplicationService dedup = new DeduplicationService();
        long[] counters = {0, 0};

        readerService.forEachLine(csvPath.toFile(), delimiter, rowData -> {
            counters[0]++;
            EmpresaRegistro registro = EmpresaRegistro.fromRow(rowData);
            if (registro == null) {
                return; // linha incompleta (paridade com legado)
            }
            if (validarDigitosVerificadores
                    ? !CnpjValidator.isValidoCompleto(registro.cnpj())
                    : !CnpjValidator.isFormatoValido(registro.cnpj())) {
                return; // API REST usa critério rígido: formato + dígitos verificadores
            }
            if (!dedup.registerIfNew(registro.cnpj())) {
                return; // duplicado no arquivo
            }
            counters[1]++;
            Operadora operadora = OperadoraClassifier.classify(registro.telefonesRaw());
            buckets.get(operadora).add(registro);
        });

        Map<String, Long> contagem = new LinkedHashMap<>();
        Map<String, List<String[]>> detalhe = new LinkedHashMap<>();
        buckets.forEach((operadora, lista) -> {
            if (!lista.isEmpty()) {
                contagem.put(operadora.fileNameToken(), (long) lista.size());
                detalhe.put(operadora.fileNameToken(),
                        lista.stream().map(EmpresaRegistro::asColumns).toList());
            }
        });

        log.info("Motor REST: {} classificou {} linhas, {} únicos ({})",
                fileName, counters[0], counters[1], contagem);

        return new Resultado(fileName, uf, counters[0], (int) counters[1], contagem, detalhe);
    }

    /** Mesma lógica de extração de UF usada pelo pipeline CLI (paridade). */
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
}
