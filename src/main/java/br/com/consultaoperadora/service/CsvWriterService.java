package br.com.consultaoperadora.service;

import br.com.consultaoperadora.model.EmpresaRegistro;
import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Escrita de CSV de saída (Fase 1) — extraído de {@code Main.exportToCSV}.
 * Formato preservado: delimitador ';', sem aspas, cabeçalho fixo de 6 colunas,
 * nome "{UF} - {OPERADORA}.csv".
 */
public final class CsvWriterService {

    private static final Logger log = LoggerFactory.getLogger(CsvWriterService.class);

    private final Path outputFolder;

    public CsvWriterService(Path outputFolder) {
        this.outputFolder = outputFolder;
    }

    /**
     * Escreve os registros no arquivo da operadora informada.
     *
     * @return 1 se um arquivo foi gerado, 0 se lista vazia ou erro de IO
     */
    public int export(String state, String operadoraFileNameToken, List<EmpresaRegistro> data) {
        if (data == null || data.isEmpty()) {
            return 0;
        }
        String fileName = state + " - " + operadoraFileNameToken + ".csv";
        Path outputPath = outputFolder.resolve(fileName);
        try (CSVWriter writer = new CSVWriter(
                new FileWriter(outputPath.toFile()),
                ';',
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            writer.writeNext(EmpresaRegistro.EXPORT_HEADER);
            writer.writeAll(data.stream().map(EmpresaRegistro::asColumns).toList());
            return 1;
        } catch (IOException e) {
            log.error("Erro ao salvar {}: {}", fileName, e.getMessage());
            return 0;
        }
    }
}
