package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.config.AppConfig;
import br.com.consultaoperadora.model.EmpresaRegistro;
import br.com.consultaoperadora.model.Operadora;
import br.com.consultaoperadora.service.CnpjValidator;
import br.com.consultaoperadora.service.CsvDelimiterDetector;
import br.com.consultaoperadora.service.CsvReaderService;
import br.com.consultaoperadora.service.CsvWriterService;
import br.com.consultaoperadora.service.DeduplicationService;
import br.com.consultaoperadora.service.OperadoraClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Orquestração do pipeline (Fase 1) — substitui o miolo de {@code Main}.
 *
 * Fluxo por arquivo (idêntico ao legado):
 *  detecção de delimitador -> leitura em streaming -> validação de CNPJ ->
 *  deduplicação -> classificação de operadora -> agrupamento -> exportação CSV.
 *
 * Arquivos são processados em paralelo (thread pool configurável); cada thread
 * usa um DeduplicationService próprio (escopo por arquivo, como no original).
 */
public final class ProcessamentoBatchService {

    private static final Logger log = LoggerFactory.getLogger(ProcessamentoBatchService.class);

    /** Ordem de escrita dos arquivos == ordem do legado (CLARO, VIVO, TIM, OI, FIXO, SEM_OPERADORA). */
    private static final List<Operadora> OPERADORAS_EM_ORDEM =
            List.of(Operadora.CLARO, Operadora.VIVO, Operadora.TIM, Operadora.OI,
                    Operadora.FIXO, Operadora.SEM_OPERADORA);

    private static final Charset INPUT_ENCODING = StandardCharsets.ISO_8859_1;
    private static final int PROGRESS_LOG_EVERY = 100_000;

    private final AppConfig config;
    private final List<String> ufsPermitidas;
    private final MetricasProcessamento metricas;
    private final CsvReaderService readerService;
    private final CsvWriterService writerService;

    public ProcessamentoBatchService(AppConfig config, MetricasProcessamento metricas) {
        this(config, metricas, config.getUfsPermitidas());
    }

    /**
     * Sobrecarga da Fase 3: permite ao backend HTTP usar um conjunto de UFs
     * diferente do config global (ex.: validação de upload por requisição).
     */
    public ProcessamentoBatchService(AppConfig config, MetricasProcessamento metricas, List<String> ufsPermitidas) {
        this.config = config;
        this.ufsPermitidas = List.copyOf(ufsPermitidas);
        this.metricas = metricas;
        this.readerService = new CsvReaderService(INPUT_ENCODING);
        this.writerService = new CsvWriterService(config.getOutputFolder());
    }

    /** Executa sobre a pasta de entrada. @return 0 se tudo ok, 1 se houve falhas. */
    public int run(File inputDir) {
        try {
            Files.createDirectories(config.getOutputFolder());
        } catch (IOException e) {
            log.error("Erro ao criar diretório de saída {}: {}", config.getOutputFolder(), e.getMessage());
            return 1;
        }

        File[] csvFiles = inputDir.listFiles((dir, name) -> isMatchingCsv(name));
        if (csvFiles == null || csvFiles.length == 0) {
            log.warn("Nenhum arquivo CSV das UFs permitidas {} encontrado com os padrões de nome esperados.",
                    ufsPermitidas);
            return 0;
        }
        Arrays.sort(csvFiles); // ordem determinística entre execuções
        log.info("Arquivos das UFs permitidas encontrados: {}", csvFiles.length);

        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadPoolSize());
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (File csvFile : csvFiles) {
                futures.add(executor.submit(() -> {
                    try {
                        processFile(csvFile);
                    } catch (Exception e) {
                        log.error("Erro não capturado ao processar arquivo: {}", csvFile.getName(), e);
                        metricas.markFileFailure();
                    }
                    return null;
                }));
            }
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    log.debug("Falha já registrada pela thread worker", e);
                }
            }
        } finally {
            executor.shutdown();
        }
        return metricas.houveFalhas() ? 1 : 0;
    }

    /** Filtra CSV cujo nome contém uma UF permitida delimitada por '_' (ex.: al_ativos.csv ou ativos_al.csv). */
    public boolean isMatchingCsv(String fileName) {
        String lowerCaseName = fileName.toLowerCase(Locale.ROOT);
        if (!lowerCaseName.endsWith(".csv")) {
            return false;
        }
        return ufsPermitidas.stream().anyMatch(uf -> {
            String lowerUf = uf.toLowerCase(Locale.ROOT);
            boolean ufPrefix = lowerCaseName.contains(lowerUf + "_");
            boolean ufSuffix = lowerCaseName.endsWith("_" + lowerUf + ".csv");
            return ufPrefix || ufSuffix;
        });
    }

    /** Extrai a UF do nome do arquivo; fallback estrutural idêntico ao legado. */
    public String extractStateFromFileName(String fileName) {
        String lowerCaseName = fileName.toLowerCase(Locale.ROOT);
        for (String uf : ufsPermitidas) {
            String lowerUf = uf.toLowerCase(Locale.ROOT);
            if (lowerCaseName.contains(lowerUf + "_") || lowerCaseName.endsWith("_" + lowerUf + ".csv")) {
                return uf;
            }
        }
        String nameWithoutExtension = fileName.replace(".csv", "").trim().toUpperCase(Locale.ROOT);
        String[] parts = nameWithoutExtension.split("_");
        if (parts.length > 0) {
            String lastPart = parts[parts.length - 1];
            if (lastPart.length() == 2 && ufsPermitidas.contains(lastPart)) {
                return lastPart;
            }
        }
        return "XX";
    }

    void processFile(File csvFile) {
        long start = System.nanoTime();
        String state = extractStateFromFileName(csvFile.getName());
        log.info("Processando: {}", csvFile.getName());

        Map<Operadora, List<EmpresaRegistro>> operadoraData = new EnumMap<>(Operadora.class);
        for (Operadora o : OPERADORAS_EM_ORDEM) {
            operadoraData.put(o, new ArrayList<>());
        }
        DeduplicationService dedup = new DeduplicationService();

        char detectedDelimiter;
        try {
            detectedDelimiter = CsvDelimiterDetector.detect(csvFile, INPUT_ENCODING);
        } catch (IOException e) {
            log.error("Erro fatal na detecção do delimitador: {} - {}", csvFile.getName(), e.getMessage());
            metricas.markFileFailure();
            return;
        }

        long[] counters = {0, 0}; // [0]=linhas totais, [1]=únicos válidos

        try {
            readerService.forEachLine(csvFile, detectedDelimiter, rowData -> {
                counters[0]++;
                EmpresaRegistro registro = EmpresaRegistro.fromRow(rowData);
                if (registro == null) {
                    return; // linha incompleta
                }
                if (counters[0] <= 5) {
                    log.debug("Linha {} extração: CNPJ='{}', Telefones='{}'",
                            counters[0], registro.cnpj(), registro.telefonesRaw());
                }
                if (!CnpjValidator.isFormatoValido(registro.cnpj())) {
                    return; // CNPJ inválido
                }
                if (!dedup.registerIfNew(registro.cnpj())) {
                    return; // duplicado no arquivo
                }
                counters[1]++;

                Operadora operadora = OperadoraClassifier.classify(registro.telefonesRaw());
                operadoraData.get(operadora).add(registro);

                if (counters[0] % PROGRESS_LOG_EVERY == 0) {
                    log.info("[{}] Progresso: {} linhas lidas", state,
                            MetricasProcessamento.formatNumber(counters[0]));
                }
            });

            long total = counters[0];
            int uniqueCount = (int) counters[1];
            log.info("[{}] Leitura concluída: {} linhas", state, MetricasProcessamento.formatNumber(total));

            int filesWritten = 0;
            for (Operadora o : OPERADORAS_EM_ORDEM) {
                List<EmpresaRegistro> bucket = operadoraData.get(o);
                metricas.addRegistroOperadora(o.fileNameToken(), bucket.size()); // Fase 3: dashboard
                filesWritten += writerService.export(state, o.fileNameToken(), bucket);
            }
            metricas.incrementCsvFilesGerados(filesWritten);

            log.info("Distribuição - CLARO: {}, VIVO: {}, TIM: {}, OI: {}, FIXO: {}, SEM OPERADORA: {}",
                    operadoraData.get(Operadora.CLARO).size(), operadoraData.get(Operadora.VIVO).size(),
                    operadoraData.get(Operadora.TIM).size(), operadoraData.get(Operadora.OI).size(),
                    operadoraData.get(Operadora.FIXO).size(), operadoraData.get(Operadora.SEM_OPERADORA).size());

            metricas.addRegistrosUnicos(uniqueCount);
            metricas.markFileSuccess();

            long duration = (System.nanoTime() - start) / 1_000_000_000;
            log.info("✓ {} - {} registros únicos processados de um total de {} ({} arquivos) em {}",
                    state, MetricasProcessamento.formatNumber(uniqueCount),
                    MetricasProcessamento.formatNumber(total), filesWritten,
                    MetricasProcessamento.formatDuration(duration));

        } catch (IOException e) {
            log.error("Erro ao processar arquivo (CSVReader falhou): {} - {}", csvFile.getName(), e.getMessage());
            metricas.markFileFailure();
        } catch (Exception e) {
            log.error("Erro inesperado ao processar arquivo: {}", csvFile.getName(), e);
            metricas.markFileFailure();
        }
    }

    public MetricasProcessamento getMetricas() {
        return metricas;
    }
}
