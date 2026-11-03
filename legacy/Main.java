import com.opencsv.CSVWriter;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParserBuilder;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberType;
import com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper;
import com.google.i18n.phonenumbers.NumberParseException;

import java.io.*;
import java.nio.file.*;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    // ** ATENÇÃO: VERIFIQUE E AJUSTE ESTES CAMINHOS **
    private static final String INPUT_FOLDER = "E:\\Projetos JAVA\\CnpjDownloader\\cnpj_data\\export";
    private static final String OUTPUT_FOLDER = "E:\\Projetos JAVA\\CnpjDownloader\\arquivos_finalizados";
    private static final String LOG_FILE = OUTPUT_FOLDER + File.separator + "processamento.log";

    private static final int THREAD_POOL_SIZE = 6;
    private static final char[] COMMON_DELIMITERS = {',', ';', '\t'}; // Vírgula, Ponto e Vírgula, Tab

    private static final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private static final AtomicInteger totalCsvFilesGenerated = new AtomicInteger(0);
    private static final AtomicInteger successfulFiles = new AtomicInteger(0);
    private static final AtomicInteger failedFiles = new AtomicInteger(0);

    private static final List<String> NORDESTE_STATES =
            Arrays.asList("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE");

    private static PrintWriter logWriter;

    public static void main(String[] args) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        try {
            Files.createDirectories(Paths.get(OUTPUT_FOLDER));
            logWriter = new PrintWriter(new FileWriter(LOG_FILE, true), true);
        } catch (IOException e) {
            System.err.println("Erro ao preparar log: " + e.getMessage());
            return;
        }

        log("=== INÍCIO DO PROCESSAMENTO === " + dtf.format(LocalDateTime.now()));

        File inputDir = new File(INPUT_FOLDER);
        if (!inputDir.exists()) {
            log("Diretório de entrada não encontrado: " + INPUT_FOLDER);
            return;
        }

        // Filtra os arquivos CSV que contêm uma UF do Nordeste (case-insensitive e delimitada por '_')
        File[] csvFiles = inputDir.listFiles((dir, name) -> {
            String lowerCaseName = name.toLowerCase();

            // 1. Deve ser um arquivo CSV
            if (!lowerCaseName.endsWith(".csv")) {
                return false;
            }

            // 2. Deve conter uma UF do Nordeste delimitada por '_' (Ex: al_ativos.csv ou ativos_al.csv)
            return NORDESTE_STATES.stream().anyMatch(uf -> {
                String lowerUf = uf.toLowerCase(); 
                
                // Procura pela UF seguida de '_' (Ex: "al_ativos.csv")
                boolean ufPrefix = lowerCaseName.contains(lowerUf + "_");
                
                // Procura pela UF precedida de '_' e seguida de '.csv' (Ex: "ativos_al.csv")
                boolean ufSuffix = lowerCaseName.endsWith("_" + lowerUf + ".csv");
                
                return ufPrefix || ufSuffix;
            });
        });

        if (csvFiles == null || csvFiles.length == 0) {
            log("Nenhum arquivo CSV do Nordeste encontrado com os padrões de nome esperados.");
            return;
        }

        log("Arquivos do Nordeste encontrados: " + csvFiles.length);

        long startTime = System.nanoTime();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<Void>> futures = new ArrayList<>();

        for (File csvFile : csvFiles) {
            futures.add(executor.submit(() -> {
                try {
                    processFile(csvFile);
                } catch (Exception e) {
                    log("Erro não capturado ao processar arquivo: " + csvFile.getName() + " - " + e.getMessage());
                    failedFiles.incrementAndGet();
                }
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // Erro já logado na thread worker
            }
        }

        executor.shutdown();

        long duration = (System.nanoTime() - startTime) / 1_000_000_000;
        NumberFormat nf = NumberFormat.getNumberInstance(new Locale("pt", "BR"));

        log("\n=== RELATÓRIO FINAL ===");
        log("Tempo total: " + formatDuration(duration));
        log("Arquivos processados com sucesso: " + successfulFiles.get() + "/" + csvFiles.length);
        log("Registros processados: " + nf.format(totalRecordsProcessed.get()));
        log("Arquivos CSV gerados: " + totalCsvFilesGenerated.get());
        log("Taxa de sucesso: " + String.format("%.1f", (successfulFiles.get() * 100.0) / csvFiles.length) + "%");
        log("=== FIM DO PROCESSAMENTO ===");

        logWriter.close();
    }
    
    // --- NOVO MÉTODO PARA DETECTAR O DELIMITADOR ---
    private static char detectDelimiterAndReadHeader(File csvFile) throws IOException {
        char bestDelimiter = ','; // Padrão
        int maxColumns = 0;
        
        // Lê a primeira linha (cabeçalho)
        String headerLine;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "ISO-8859-1"))) {
            headerLine = br.readLine();
        }
        
        if (headerLine == null) {
            throw new IOException("Arquivo vazio ou falha na leitura da primeira linha.");
        }

        log("--- Tentando detectar delimitador para: " + csvFile.getName() + " ---");

        for (char delimiter : COMMON_DELIMITERS) {
            // Tenta analisar a linha com o delimitador atual
            String[] parts = headerLine.split(Pattern.quote(String.valueOf(delimiter)));
            int columnCount = parts.length;

            if (columnCount > maxColumns) {
                maxColumns = columnCount;
                bestDelimiter = delimiter;
            }
            log(String.format("Tentativa com '%s' (%s): %d colunas", delimiter == '\t' ? "TAB" : delimiter, delimiter, columnCount));
        }

        if (maxColumns < 6) { // Se nem o melhor delimitador encontrou as 6 colunas mínimas
             log("Atenção: A detecção automática encontrou apenas " + maxColumns + " colunas. Usando o melhor palpite: " + bestDelimiter);
             // Não lançamos exceção, mas deixamos o processFile tentar com o melhor palpite.
        } else {
             log("Delimitador detectado: " + bestDelimiter + " (encontradas " + maxColumns + " colunas).");
        }
        
        return bestDelimiter;
    }
    // --- FIM NOVO MÉTODO ---


    private static void processFile(File csvFile) {
        long start = System.nanoTime();
        String state = extractStateFromFileName(csvFile.getName());
        NumberFormat nf = NumberFormat.getNumberInstance(new Locale("pt", "BR"));

        log("Processando: " + csvFile.getName());

        Map<String, List<String[]>> operadoraData = new HashMap<>();
        operadoraData.put("CLARO", new ArrayList<>());
        operadoraData.put("VIVO", new ArrayList<>());
        operadoraData.put("TIM", new ArrayList<>());
        operadoraData.put("OI", new ArrayList<>());
        operadoraData.put("FIXO", new ArrayList<>());
        operadoraData.put("SEM OPERADORA", new ArrayList<>());
        
        Set<String> processedCnpjs = new HashSet<>();

        int total = 0;
        int uniqueCount = 0;

        // Cabeçalho fixo com as 6 colunas originais
        String[] header = {"cnpj_completo", "razao_social", "endereco_completo", "email", "ano_abertura", "telefones"};
        
        // 1. Detectar o delimitador
        char detectedDelimiter;
        try {
             detectedDelimiter = detectDelimiterAndReadHeader(csvFile);
        } catch (IOException e) {
             log("Erro fatal na detecção do delimitador: " + csvFile.getName() + " - " + e.getMessage());
             failedFiles.incrementAndGet();
             return;
        }

        // 2. Processamento principal
        try (CSVReader reader = new CSVReaderBuilder(
                     new InputStreamReader(new FileInputStream(csvFile), "ISO-8859-1"))
                     // USA O DELIMITADOR DETECTADO
                     .withCSVParser(new CSVParserBuilder().withSeparator(detectedDelimiter).withQuoteChar('"').build()) 
                     .build()) {
            
            // Pula o cabeçalho original (que já foi lido para detecção)
            reader.readNext();  
            
            String[] rowData; // Os dados crus da linha
            
            log("--- DEBUG DE EXTRAÇÃO ---");

            // Loop principal
            while ((rowData = reader.readNext()) != null) {
                total++;

                // A unificação do ETL original gera muitas colunas, mas você só precisa das 6 primeiras
                if (rowData.length < 6) {
                    // Linha incompleta
                    continue; 
                }
                
                // Extrai as 6 colunas que você está usando (CNPJ completo, ..., Telefones)
                String[] rowDataForExport = Arrays.copyOfRange(rowData, 0, 6);
                
                // Bloco de DEBUG para as primeiras linhas
                if (total <= 5) {
                    log(String.format("Linha %d (Sucesso Extração): CNPJ='%s', Telefones='%s'", total, rowDataForExport[0], rowDataForExport[5]));
                }
                // Fim do bloco de DEBUG
                
                String cnpj = rowDataForExport[0].trim();
                
                // 1. Validação de CNPJ: Deve ser 14 dígitos
                if (cnpj.isEmpty() || !cnpj.matches("^\\d{14}$")) { 
                    continue; // Descarta CNPJ inválido
                }

                // 2. Validação de Duplicidade
                if (!processedCnpjs.add(cnpj)) {
                    continue; // Descarta CNPJ duplicado
                }

                uniqueCount++;

                String telefonesRaw = rowDataForExport[5];
                
                String operadora = getOperadora(telefonesRaw);
                
                String key = "SEM OPERADORA";
                if (operadora.equals("CLARO")) key = "CLARO";
                else if (operadora.equals("VIVO")) key = "VIVO";
                else if (operadora.equals("TIM")) key = "TIM";
                else if (operadora.equals("OI")) key = "OI";
                else if (operadora.equals("FIXO")) key = "FIXO";

                operadoraData.get(key).add(rowDataForExport);
                
                // Progresso a cada 100.000 linhas
                if (total % 100000 == 0) {
                    showProgress(state, total, total); 
                }
            }
            log("--- FIM DO DEBUG ---"); 
            
            // Mostra o progresso final
            showProgress(state, total, total);

            // Exporta os arquivos
            int filesWritten = 0;
            filesWritten += exportToCSV(state, "CLARO", operadoraData.get("CLARO"), header);
            filesWritten += exportToCSV(state, "VIVO", operadoraData.get("VIVO"), header);
            filesWritten += exportToCSV(state, "TIM", operadoraData.get("TIM"), header);
            filesWritten += exportToCSV(state, "OI", operadoraData.get("OI"), header);
            filesWritten += exportToCSV(state, "FIXO", operadoraData.get("FIXO"), header);
            filesWritten += exportToCSV(state, "SEM_OPERADORA", operadoraData.get("SEM OPERADORA"), header);

            // Log das quantidades por operadora
            log("Distribuição - CLARO: " + operadoraData.get("CLARO").size() + 
                ", VIVO: " + operadoraData.get("VIVO").size() +
                ", TIM: " + operadoraData.get("TIM").size() +
                ", OI: " + operadoraData.get("OI").size() +
                ", FIXO: " + operadoraData.get("FIXO").size() +
                ", SEM OPERADORA: " + operadoraData.get("SEM OPERADORA").size());

            totalRecordsProcessed.addAndGet(uniqueCount);
            successfulFiles.incrementAndGet();

            long duration = (System.nanoTime() - start) / 1_000_000_000;
            log("✓ " + state + " - " + nf.format(uniqueCount) + " registros únicos processados de um total de " + nf.format(total) + " (" + filesWritten + " arquivos) em " + formatDuration(duration));

        } catch (IOException e) {
            log("Erro ao processar arquivo (CSVReader falhou): " + csvFile.getName() + " - " + e.getMessage());
            failedFiles.incrementAndGet();
        } catch (Exception e) {
            log("Erro inesperado ao processar arquivo: " + csvFile.getName() + " - " + e.getMessage());
            e.printStackTrace(logWriter);
            failedFiles.incrementAndGet();
        }
    }

    // *** MÉTODOS DE TELEFONE (Ajustado o Regex para suportar o formato 82-33111200) ***

    private static String normalizeFirstPhoneNumber(String telefonesRaw) {
        if (telefonesRaw == null || telefonesRaw.trim().isEmpty() || telefonesRaw.contains("()")) {
            return "";
        }
        
        // Novo Regex flexível: Procura por (DD) NNNN-NNNN OU DD-NNNNNNNN (o formato que você forneceu)
        Pattern flexNumberPattern = Pattern.compile("(\\d{2})[- ]*(\\d{4,5})[- ]*(\\d{4})|\\((\\d{2})\\)\\s*([\\d\\s-]+)");
        Matcher matcher = flexNumberPattern.matcher(telefonesRaw);

        String ddd = null;
        String numero = null;

        if (matcher.find()) {
            // Grupo 1 e 2/3: Formato DD-NNNNNNNN (ex: 82-33111200)
            if (matcher.group(1) != null) { 
                ddd = matcher.group(1);
                // Concatena as partes do número: NNNN/NNNNN + NNNN
                numero = matcher.group(2) + matcher.group(3); 
            } 
            // Grupo 4 e 5: Formato (DD) NNNN-NNNN (formato anterior)
            else if (matcher.group(4) != null) {
                ddd = matcher.group(4);
                numero = matcher.group(5).replaceAll("[^\\d]", "");
            }
        }
        
        if (ddd == null || numero == null) {
             return "";
        }
        
        // Remove caracteres não-dígitos
        numero = numero.replaceAll("[^\\d]", "");
            
        // Aplica a regra do nono dígito (se for 8 dígitos e começar com 6, 7, 8 ou 9)
        if (numero.length() == 8 && numero.matches("^[6-9]\\d{7}$")) {
            numero = "9" + numero;
        }
            
        // Retorna no formato internacional (Country Code + DDD + Número)
        return "55" + ddd + numero;
    }
    
    // ... (restante dos métodos de telefone getOperadora, identifyCarrier, identifyCarrierFallback, etc., inalterados) ...
    // ... (métodos auxiliares extractStateFromFileName, exportToCSV, showProgress, log, formatDuration, inalterados) ...

    private static String getOperadora(String telefonesRaw) {
        // ... (inalterado) ...
        String fullNumber = normalizeFirstPhoneNumber(telefonesRaw);

        if (fullNumber.isEmpty() || fullNumber.length() < 12) {
            return "SEM OPERADORA";
        }

        try {
            PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
            // Passamos o número com '55' e a região 'BR'
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
            // Em caso de falha da lib (ex: falta do carrier.jar), tenta o fallback
            return identifyCarrierFallback(fullNumber);
        }
    }

    private static String identifyCarrier(PhoneNumber numberProto, String fullNumber) {
        try {
            PhoneNumberToCarrierMapper carrierMapper = PhoneNumberToCarrierMapper.getInstance();
            String carrier = carrierMapper.getNameForNumber(numberProto, Locale.forLanguageTag("pt-BR"));
            
            if (carrier != null && !carrier.isEmpty()) {
                 String upperCarrier = carrier.toUpperCase();
                 if (upperCarrier.contains("CLARO")) return "CLARO";
                 if (upperCarrier.contains("VIVO")) return "VIVO";
                 if (upperCarrier.contains("TIM")) return "TIM";
                 if (upperCarrier.contains("OI")) return "OI";
                 
                 // Retorna outras operadoras (Nextel, Algar, etc.)
                 return upperCarrier.replaceAll("[^A-Z]", "");  
            }
        } catch (Exception e) {
            // Se a lib falhar aqui, cai no fallback
        }
        
        return identifyCarrierFallback(fullNumber);
    }

    private static String identifyCarrierFallback(String fullNumber) {
        // fullNumber tem pelo menos 12 dígitos (55 + DDD + Número)
        if (fullNumber.length() < 12) {
            return "SEM OPERADORA";  
        }
        
        // Verifica se é um móvel de 13 dígitos (55 + DDD + 9 + 8 dígitos)
        if (fullNumber.length() == 13) {
             // O dígito de identificação é o 4º dígito do número (índice 4 no fullNumber)
             String digitoIdentificador = fullNumber.substring(4, 5);  

             // Esta lógica é uma estimativa baseada em faixas antigas de DDD, pode ser imprecisa!
             switch (digitoIdentificador) {
                 case "6":
                 case "7":
                     return "CLARO"; 
                 case "9":
                     return "VIVO";
                 case "8": 
                     return "TIM";
                 case "3":
                     return "OI";
                 default:
                     return "SEM OPERADORA";
             }
        }
        
        return "SEM OPERADORA";
    }

    private static String extractStateFromFileName(String fileName) {
        
        String lowerCaseName = fileName.toLowerCase();
        
        for (String uf : NORDESTE_STATES) {
            String lowerUf = uf.toLowerCase();
            
            // 1. Caso: al_ativos.csv
            if (lowerCaseName.contains(lowerUf + "_")) {
                return uf;
            }
            // 2. Caso: ativos_al.csv (no final)
            if (lowerCaseName.endsWith("_" + lowerUf + ".csv")) {
                return uf;
            }
        }

        // Fallback: tenta extrair o último grupo de letras/números antes do .csv
        String nameWithoutExtension = fileName.replace(".csv", "").trim().toUpperCase();
        String[] parts = nameWithoutExtension.split("_");
        if (parts.length > 0) {
             String lastPart = parts[parts.length - 1];
             if (lastPart.length() == 2 && NORDESTE_STATES.contains(lastPart)) {
                 return lastPart;
             }
        }
        
        return "XX"; 
    }

    private static int exportToCSV(String state, String operadora, List<String[]> data, String[] header) {
        if (data.isEmpty()) return 0;

        String fileName = state + " - " + operadora + ".csv";
        Path outputPath = Paths.get(OUTPUT_FOLDER, fileName);

        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputPath.toFile()),
                ';', // Delimitador de saída é o ponto e vírgula
                CSVWriter.NO_QUOTE_CHARACTER, // Não usa aspas para envolver os campos
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
                
            writer.writeNext(header);
            writer.writeAll(data);
            totalCsvFilesGenerated.incrementAndGet();
            return 1;
        } catch (IOException e) {
            log("Erro ao salvar " + fileName + ": " + e.getMessage());
            return 0;
        }
    }

    private static void showProgress(String state, int done, int total) {
        int percent = (total > 0) ? (int) ((done * 100.0) / total) : 0;
        // Usa \r para reescrever na mesma linha
        System.out.printf("\r[%s] Progresso: %s linhas lidas (%d%%)", state, NumberFormat.getNumberInstance(new Locale("pt", "BR")).format(done), percent);
        if (done == total) System.out.println();
    }

    private static void log(String msg) {
        String time = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss").format(LocalDateTime.now());
        String logMsg = "[" + time + "] " + msg;
        System.out.println(logMsg);
        logWriter.println(logMsg);
    }

    private static String formatDuration(long totalSeconds) {
        long h = totalSeconds / 3600;
        long m = (totalSeconds % 3600) / 60;
        long s = totalSeconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }
}