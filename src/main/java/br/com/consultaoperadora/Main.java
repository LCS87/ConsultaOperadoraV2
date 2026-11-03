package br.com.consultaoperadora;

import br.com.consultaoperadora.config.AppConfig;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberType;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bootstrap CLI do processador de CSVs CNPJ x Operadora.
 *
 * Fase 0 do plano de melhoria:
 *  - Configuração externa via AppConfig (sem caminhos hardcoded);
 *  - Logs via SLF4J/Logback (sem PrintWriter manual);
 *  - Comportamento identico ao Main.java legado (preservado em legacy/Main.java).
 *
 * A quebra em servicos dedicados (CnpjValidator, OperadoraClassifier, etc.)
 * acontece na Fase 1.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final char[] COMMON_DELIMITERS = {',', ';', '\t'};
    private static final String INPUT_ENCODING = "ISO-8859-1";
    private static final String[] EXPORT_HEADER = {
            "cnpj_completo", "razao_social", "endereco_completo", "email", "ano_abertura", "telefones"};

    private static final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private static final AtomicInteger totalCsvFilesGenerated = new AtomicInteger(0);
    private static final AtomicInteger successfulFiles = new AtomicInteger(0);
    private static final AtomicInteger failedFiles = new AtomicInteger(0);

    private final AppConfig config;
    private final List<String> ufsPermitidas;

    public Main(AppConfig config) {
        this.config = config;
        this.ufsPermitidas = config.getUfsPermitidas();
    }

    public static void main(String[] args) {
        AppConfig config;
        try {
            config = AppConfig.load();
        } catch (IllegalStateException e) {
            System.err.println("Configuração inválida: " + e.getMessage());
            System.err.println("Use --help para ver as opções.");
            if (args.length == 0 || !"--help".equals(args[0])) {
                printUsage();
                System.exit(2);
            }
            return;
        }

        if (args.length > 0 && "--help".equals(args[0])) {
            printUsage();
            return;
        }

        // Permite override por argumento posicional: java -jar app.jar <input> [output]
        if (args.length >= 1) {
            config = reloadWithOverrides(config, args);
        }

        log.info("=== INÍCIO DO PROCESSAMENTO === {}", config);

        File inputDir = config.getInputFolder().toFile();
        if (!inputDir.exists()) {
            log.error("Diretório de entrada não encontrado: {}", config.getInputFolder());
            System.exit(1);
            return;
        }

        long startTime = System.nanoTime();
        int exitCode = new Main(config).run(inputDir);
        long durationSec = (System.nanoTime() - startTime) / 1_000_000_000;

        NumberFormat nf = NumberFormat.getNumberInstance(new Locale("pt", "BR"));
        int found = successfulFiles.get() + failedFiles.get();
        log.info("=== RELATÓRIO FINAL ===");
        log.info("Tempo total: {}", formatDuration(durationSec));
        log.info("Arquivos processados com sucesso: {}/{}", successfulFiles.get(), found);
        log.info("Registros processados: {}", nf.format(totalRecordsProcessed.get()));
        log.info("Arquivos CSV gerados: {}", totalCsvFilesGenerated.get());
        if (found > 0) {
            log.info("Taxa de sucesso: {}", String.format("%.1f%%", (successfulFiles.get() * 100.0) / found));
        }
        log.info("=== FIM DO PROCESSAMENTO ===");

        System.exit(exitCode == 0 ? 0 : 1);
    }

    private static AppConfig reloadWithOverrides(AppConfig base, String[] args) {
        System.setProperty("app.input-folder", args[0]);
        if (args.length >= 2) {
            System.setProperty("app.output-folder", args[1]);
        }
        return AppConfig.load();
    }

    private static void printUsage() {
        System.out.println("""
                Uso: java -jar consulta-operadora.jar [INPUT_FOLDER] [OUTPUT_FOLDER]

                Configuração (precedência: variável de ambiente > -Dpropriedade > application.properties):
                  app.input-folder       Pasta com os CSVs de entrada   (var: APP_INPUT_FOLDER)
                  app.output-folder      Pasta de saída dos resultados  (var: APP_OUTPUT_FOLDER)
                  app.thread-pool-size   Threads paralelas (default 6)  (var: APP_THREAD_POOL_SIZE)
                  app.ufs-permitidas     UFs aceitas, separadas por vírgula (var: APP_UFS_PERMITIDAS)
                """);
    }

    /**
     * Executa o pipeline sobre a pasta de entrada. Retorna 0 se tudo ok, 1 se houve falhas.
     */
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
        List<Future<Void>> futures = new ArrayList<>();
        for (File csvFile : csvFiles) {
            futures.add(executor.submit(() -> {
                try {
                    processFile(csvFile);
                } catch (Exception e) {
                    log.error("Erro não capturado ao processar arquivo: {}", csvFile.getName(), e);
                    failedFiles.incrementAndGet();
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
        executor.shutdown();
        return failedFiles.get() > 0 ? 1 : 0;
    }

    /** Filtra CSV cujo nome contém uma UF permitida delimitada por '_' (ex.: al_ativos.csv ou ativos_al.csv). */
    boolean isMatchingCsv(String fileName) {
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

    /** Detecta o delimitador mais provável analisando a linha de cabeçalho. */
    char detectDelimiter(File csvFile) throws IOException {
        char bestDelimiter = ',';
        int maxColumns = 0;

        String headerLine;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFile), INPUT_ENCODING))) {
            headerLine = br.readLine();
        }
        if (headerLine == null) {
            throw new IOException("Arquivo vazio ou falha na leitura da primeira linha.");
        }

        log.info("--- Tentando detectar delimitador para: {} ---", csvFile.getName());
        for (char delimiter : COMMON_DELIMITERS) {
            String[] parts = headerLine.split(Pattern.quote(String.valueOf(delimiter)));
            int columnCount = parts.length;
            if (columnCount > maxColumns) {
                maxColumns = columnCount;
                bestDelimiter = delimiter;
            }
            log.info("Tentativa com '{}': {} colunas", delimiter == '\t' ? "TAB" : delimiter, columnCount);
        }

        if (maxColumns < 6) {
            log.warn("Atenção: detecção automática encontrou apenas {} colunas. Usando melhor palpite: '{}'",
                    maxColumns, bestDelimiter);
        } else {
            log.info("Delimitador detectado: '{}' (encontradas {} colunas).", bestDelimiter, maxColumns);
        }
        return bestDelimiter;
    }

    void processFile(File csvFile) {
        long start = System.nanoTime();
        String state = extractStateFromFileName(csvFile.getName());
        NumberFormat nf = NumberFormat.getNumberInstance(new Locale("pt", "BR"));
        log.info("Processando: {}", csvFile.getName());

        Map<String, List<String[]>> operadoraData = new HashMap<>();
        for (String key : List.of("CLARO", "VIVO", "TIM", "OI", "FIXO", "SEM OPERADORA")) {
            operadoraData.put(key, new ArrayList<>());
        }
        Set<String> processedCnpjs = new HashSet<>();

        int total = 0;
        int uniqueCount = 0;

        char detectedDelimiter;
        try {
            detectedDelimiter = detectDelimiter(csvFile);
        } catch (IOException e) {
            log.error("Erro fatal na detecção do delimitador: {} - {}", csvFile.getName(), e.getMessage());
            failedFiles.incrementAndGet();
            return;
        }

        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(new FileInputStream(csvFile), INPUT_ENCODING))
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(detectedDelimiter)
                        .withQuoteChar('"')
                        .build())
                .build()) {

            reader.readNext(); // pula cabeçalho original

            String[] rowData;
            while ((rowData = reader.readNext()) != null) {
                total++;

                if (rowData.length < 6) {
                    continue; // linha incompleta
                }
                String[] rowDataForExport = Arrays.copyOfRange(rowData, 0, 6);

                if (total <= 5) {
                    log.debug("Linha {} extração: CNPJ='{}', Telefones='{}'",
                            total, rowDataForExport[0], rowDataForExport[5]);
                }

                String cnpj = rowDataForExport[0].trim();
                if (cnpj.isEmpty() || !cnpj.matches("^\\d{14}$")) {
                    continue; // CNPJ inválido
                }
                if (!processedCnpjs.add(cnpj)) {
                    continue; // duplicado no arquivo
                }
                uniqueCount++;

                String operadora = getOperadora(rowDataForExport[5]);
                String key = switch (operadora) {
                    case "CLARO", "VIVO", "TIM", "OI", "FIXO" -> operadora;
                    default -> "SEM OPERADORA";
                };
                operadoraData.get(key).add(rowDataForExport);

                if (total % 100_000 == 0) {
                    log.info("[{}] Progresso: {} linhas lidas", state, nf.format(total));
                }
            }

            log.info("[{}] Leitura concluída: {} linhas", state, nf.format(total));

            int filesWritten = 0;
            filesWritten += exportToCSV(state, "CLARO", operadoraData.get("CLARO"));
            filesWritten += exportToCSV(state, "VIVO", operadoraData.get("VIVO"));
            filesWritten += exportToCSV(state, "TIM", operadoraData.get("TIM"));
            filesWritten += exportToCSV(state, "OI", operadoraData.get("OI"));
            filesWritten += exportToCSV(state, "FIXO", operadoraData.get("FIXO"));
            filesWritten += exportToCSV(state, "SEM_OPERADORA", operadoraData.get("SEM OPERADORA"));

            log.info("Distribuição - CLARO: {}, VIVO: {}, TIM: {}, OI: {}, FIXO: {}, SEM OPERADORA: {}",
                    operadoraData.get("CLARO").size(), operadoraData.get("VIVO").size(),
                    operadoraData.get("TIM").size(), operadoraData.get("OI").size(),
                    operadoraData.get("FIXO").size(), operadoraData.get("SEM OPERADORA").size());

            totalRecordsProcessed.addAndGet(uniqueCount);
            successfulFiles.incrementAndGet();

            long duration = (System.nanoTime() - start) / 1_000_000_000;
            log.info("✓ {} - {} registros únicos processados de um total de {} ({} arquivos) em {}",
                    state, nf.format(uniqueCount), nf.format(total), filesWritten, formatDuration(duration));

        } catch (IOException e) {
            log.error("Erro ao processar arquivo (CSVReader falhou): {} - {}", csvFile.getName(), e.getMessage());
            failedFiles.incrementAndGet();
        } catch (Exception e) {
            log.error("Erro inesperado ao processar arquivo: {}", csvFile.getName(), e);
            failedFiles.incrementAndGet();
        }
    }

    // --- Regras de telefone (idênticas ao legado) ---

    static String normalizeFirstPhoneNumber(String telefonesRaw) {
        if (telefonesRaw == null || telefonesRaw.trim().isEmpty() || telefonesRaw.contains("()")) {
            return "";
        }
        Pattern flexNumberPattern = Pattern.compile(
                "(\\d{2})[- ]*(\\d{4,5})[- ]*(\\d{4})|\\((\\d{2})\\)\\s*([\\d\\s-]+)");
        Matcher matcher = flexNumberPattern.matcher(telefonesRaw);

        String ddd = null;
        String numero = null;

        if (matcher.find()) {
            if (matcher.group(1) != null) {          // formato DD-NNNNNNNN (ex: 82-33111200)
                ddd = matcher.group(1);
                numero = matcher.group(2) + matcher.group(3);
            } else if (matcher.group(4) != null) {   // formato (DD) NNNN-NNNN
                ddd = matcher.group(4);
                numero = matcher.group(5).replaceAll("[^\\d]", "");
            }
        }
        if (ddd == null || numero == null) {
            return "";
        }
        numero = numero.replaceAll("[^\\d]", "");

        // Regra do nono dígito: 8 dígitos começando com 6-9 => prefixa 9
        if (numero.length() == 8 && numero.matches("^[6-9]\\d{7}$")) {
            numero = "9" + numero;
        }
        return "55" + ddd + numero;
    }

    static String getOperadora(String telefonesRaw) {
        String fullNumber = normalizeFirstPhoneNumber(telefonesRaw);
        if (fullNumber.isEmpty() || fullNumber.length() < 12) {
            return "SEM OPERADORA";
        }
        try {
            PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
            PhoneNumber numberProto = phoneUtil.parse(fullNumber, "BR");
            if (!phoneUtil.isValidNumber(numberProto)) {
                return "SEM OPERADORA";
            }
            PhoneNumberType type = phoneUtil.getNumberType(numberProto);
            if (type == PhoneNumberType.FIXED_LINE) {
                return "FIXO";
            }
            if (type == PhoneNumberType.MOBILE || type == PhoneNumberType.FIXED_LINE_OR_MOBILE) {
                return identifyCarrier(numberProto, fullNumber);
            }
            return "SEM OPERADORA";
        } catch (NumberParseException e) {
            return "SEM OPERADORA";
        } catch (Exception e) {
            return identifyCarrierFallback(fullNumber);
        }
    }

    static String identifyCarrier(PhoneNumber numberProto, String fullNumber) {
        try {
            PhoneNumberToCarrierMapper carrierMapper = PhoneNumberToCarrierMapper.getInstance();
            String carrier = carrierMapper.getNameForNumber(numberProto, Locale.forLanguageTag("pt-BR"));
            if (carrier != null && !carrier.isEmpty()) {
                String upperCarrier = carrier.toUpperCase(Locale.ROOT);
                if (upperCarrier.contains("CLARO")) return "CLARO";
                if (upperCarrier.contains("VIVO")) return "VIVO";
                if (upperCarrier.contains("TIM")) return "TIM";
                if (upperCarrier.contains("OI")) return "OI";
                return upperCarrier.replaceAll("[^A-Z]", "");
            }
        } catch (Exception e) {
            log.debug("Carrier mapper falhou, usando fallback", e);
        }
        return identifyCarrierFallback(fullNumber);
    }

    static String identifyCarrierFallback(String fullNumber) {
        if (fullNumber.length() < 12) {
            return "SEM OPERADORA";
        }
        if (fullNumber.length() == 13) {
            String digitoIdentificador = fullNumber.substring(4, 5);
            return switch (digitoIdentificador) {
                case "6", "7" -> "CLARO";
                case "9" -> "VIVO";
                case "8" -> "TIM";
                case "3" -> "OI";
                default -> "SEM OPERADORA";
            };
        }
        return "SEM OPERADORA";
    }

    String extractStateFromFileName(String fileName) {
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

    int exportToCSV(String state, String operadora, List<String[]> data) {
        if (data.isEmpty()) {
            return 0;
        }
        String fileName = state + " - " + operadora + ".csv";
        Path outputPath = config.getOutputFolder().resolve(fileName);
        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputPath.toFile()),
                ';',
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            writer.writeNext(EXPORT_HEADER);
            writer.writeAll(data);
            totalCsvFilesGenerated.incrementAndGet();
            return 1;
        } catch (IOException e) {
            log.error("Erro ao salvar {}: {}", fileName, e.getMessage());
            return 0;
        }
    }

    static String formatDuration(long totalSeconds) {
        long h = totalSeconds / 3600;
        long m = (totalSeconds % 3600) / 60;
        long s = totalSeconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }
}
