package br.com.consultaoperadora.service;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * Leitura de CSV (Fase 1) — extraído do bloco try-with-resources de
 * {@code Main.processFile}. Encoding ISO-8859-1 e aspas duplas, iguais ao legado.
 *
 * Uso em streaming (callback por linha) para não reter arquivos gigantes em memória:
 * <pre>
 *   readerService.forEachLine(file, ';', linha -> { ... });
 * </pre>
 */
public final class CsvReaderService {

    private final Charset inputEncoding;

    public CsvReaderService(Charset inputEncoding) {
        this.inputEncoding = inputEncoding;
    }

    /**
     * Lê o CSV ignorando a primeira linha (cabeçalho original) e invoca
     * {@code onRow} para cada linha restante, na ordem do arquivo.
     *
     * @return número de linhas passadas ao callback (sem contar o cabeçalho)
     */
    public long forEachLine(File csvFile, char delimiter, Consumer<String[]> onRow) throws IOException {
        try {
            return readLines(csvFile, delimiter, onRow);
        } catch (com.opencsv.exceptions.CsvValidationException e) {
            // Envelope com o nome do arquivo para diagnóstico (contrato testado na Fase 2)
            throw new IOException("Falha de validação CSV em " + csvFile.getName() + ": " + e.getMessage(), e);
        }
    }

    private long readLines(File csvFile, char delimiter, Consumer<String[]> onRow)
            throws IOException, com.opencsv.exceptions.CsvValidationException {
        long count = 0;
        try (CSVReader reader = new CSVReaderBuilder(
                new InputStreamReader(new FileInputStream(csvFile), inputEncoding))
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(delimiter)
                        .withQuoteChar('"')
                        .build())
                .build()) {

            reader.readNext(); // pula cabeçalho original

            String[] rowData;
            while ((rowData = reader.readNext()) != null) {
                count++;
                onRow.accept(rowData);
            }
        } catch (com.opencsv.exceptions.CsvMalformedLineException e) {
            // Subclasse de IOException lançada direto pelo parser (aspas não fechadas):
            // embrulhamos para preservar o contrato de diagnóstico com nome do arquivo.
            throw new IOException("Falha de validação CSV em " + csvFile.getName() + ": " + e.getMessage(), e);
        }
        return count;
    }
}
