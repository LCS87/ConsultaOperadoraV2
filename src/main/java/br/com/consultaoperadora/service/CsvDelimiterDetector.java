package br.com.consultaoperadora.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * Detecção de delimitador de CSV (Fase 1) — extraído de {@code Main.detectDelimiter}.
 * Analisa a linha de cabeçalho e escolhe o candidato com mais colunas.
 */
public final class CsvDelimiterDetector {

    private static final Logger log = LoggerFactory.getLogger(CsvDelimiterDetector.class);

    static final char[] COMMON_DELIMITERS = {',', ';', '\t'};
    static final int EXPECTED_MIN_COLUMNS = 6;

    private CsvDelimiterDetector() {
    }

    public static char detect(File csvFile, Charset encoding) throws IOException {
        String headerLine;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(csvFile), encoding))) {
            headerLine = br.readLine();
        }
        return detectFromHeader(headerLine, csvFile.getName());
    }

    /** Visível para testes unitários sem precisar de arquivo em disco. */
    static char detectFromHeader(String headerLine, String fileNameForLog) throws IOException {
        if (headerLine == null) {
            throw new IOException("Arquivo vazio ou falha na leitura da primeira linha.");
        }

        char bestDelimiter = ',';
        int maxColumns = 0;

        log.info("--- Tentando detectar delimitador para: {} ---", fileNameForLog);
        for (char delimiter : COMMON_DELIMITERS) {
            String[] parts = headerLine.split(Pattern.quote(String.valueOf(delimiter)));
            int columnCount = parts.length;
            if (columnCount > maxColumns) {
                maxColumns = columnCount;
                bestDelimiter = delimiter;
            }
            log.info("Tentativa com '{}': {} colunas", delimiter == '\t' ? "TAB" : delimiter, columnCount);
        }

        if (maxColumns < EXPECTED_MIN_COLUMNS) {
            log.warn("Atenção: detecção automática encontrou apenas {} colunas. Usando melhor palpite: '{}'",
                    maxColumns, bestDelimiter);
        } else {
            log.info("Delimitador detectado: '{}' (encontradas {} colunas).", bestDelimiter, maxColumns);
        }
        return bestDelimiter;
    }
}
