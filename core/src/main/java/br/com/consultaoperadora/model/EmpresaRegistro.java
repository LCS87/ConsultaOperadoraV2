package br.com.consultaoperadora.model;

import java.util.Arrays;

/**
 * DTO imutável de um registro lido do CSV de entrada (Fase 1).
 *
 * Substitui o {@code String[6]} cru que transitava pelo Main legado.
 * As colunas seguem a ordem do arquivo de exportação:
 * cnpj_completo, razao_social, endereco_completo, email, ano_abertura, telefones.
 */
public record EmpresaRegistro(String[] columns) {

    public static final int EXPECTED_MIN_COLUMNS = 6;
    public static final String[] EXPORT_HEADER = {
            "cnpj_completo", "razao_social", "endereco_completo", "email", "ano_abertura", "telefones"};

    /** Extrai as 6 primeiras colunas; retorna null se a linha for incompleta (paridade com legado). */
    public static EmpresaRegistro fromRow(String[] rowData) {
        if (rowData == null || rowData.length < EXPECTED_MIN_COLUMNS) {
            return null;
        }
        return new EmpresaRegistro(Arrays.copyOfRange(rowData, 0, EXPECTED_MIN_COLUMNS));
    }

    public String cnpj() {
        return columns[0].trim();
    }

    public String razaoSocial() {
        return columns[1];
    }

    public String enderecoCompleto() {
        return columns[2];
    }

    public String email() {
        return columns[3];
    }

    public String anoAbertura() {
        return columns[4];
    }

    /** Coluna bruta de telefones (entrada do TelefoneNormalizer/OperadoraClassifier). */
    public String telefonesRaw() {
        return columns[5];
    }

    /** Cópia defensiva das colunas — usada pelo CsvWriterService. */
    public String[] asColumns() {
        return columns.clone();
    }
}
