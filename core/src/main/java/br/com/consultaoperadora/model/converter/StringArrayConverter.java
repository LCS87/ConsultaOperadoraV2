package br.com.consultaoperadora.model.converter;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * Converte o array legado de 6 colunas do CSV para uma única célula TEXT e
 * vice-versa (Fase 4 — Persistência).
 *
 * Formato de serialização: união das colunas com o separador unitário
 * {@code \u001F} (ASCII Unit Separator), que nunca aparece nos dados de origem
 * (CSV usa ';' ou ',' como delimitadores). Na desserialização usamos
 * {@code split(-1)} para preservar colunas vazias nas pontas — essencial para
 * manter o contrato posicional de {@code EmpresaRegistro.EXPECTED_MIN_COLUMNS}.
 */
@Converter
public class StringArrayConverter implements AttributeConverter<String[], String> {

    private static final String SEP = "\u001F";

    @Override
    public String convertToDatabaseColumn(String[] attribute) {
        if (attribute == null || attribute.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attribute.length; i++) {
            if (i > 0) {
                sb.append(SEP);
            }
            sb.append(attribute[i] == null ? "" : attribute[i]);
        }
        return sb.toString();
    }

    @Override
    public String[] convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        return dbData.split(SEP, -1);
    }
}
