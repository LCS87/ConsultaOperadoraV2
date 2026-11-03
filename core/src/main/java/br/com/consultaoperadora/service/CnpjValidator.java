package br.com.consultaoperadora.service;

import java.util.regex.Pattern;

/**
 * Validação de CNPJ (Fase 1).
 *
 * IMPORTANTE — paridade com o legado: o Main original validava apenas o formato
 * "14 dígitos" ({@code matches("^\d{14}$")}). Manter esse comportamento é a regra
 * da Fase 1 ("comportamento idêntico"). A validação completa de dígitos
 * verificadores existe como método adicional ({@link #digitosVerificadoresValidos})
 * e só será ativada quando o plano decidir, protegida por teste de regressão.
 */
public final class CnpjValidator {

    private static final Pattern DEZ_QUATRO_DIGITOS = Pattern.compile("^\\d{14}$");

    private CnpjValidator() {
    }

    /** Critério usado pelo pipeline hoje: 14 dígitos (com ou sem máscara oficial). */
    public static boolean isFormatoValido(String cnpj) {
        String digitos = apenasDigitos(cnpj);
        return !digitos.isEmpty() && DEZ_QUATRO_DIGITOS.matcher(digitos).matches();
    }

    /**
     * Remove máscara e retorna os dígitos. Para preservar a paridade com o legado,
     * só aceita entradas que sejam puramente dígitos OU contenham exclusivamente
     * os separadores da máscara oficial ("." , "/" , "-") — qualquer outro caractere
     * (letras, espaços, símbolos) invalida.
     */
    public static String apenasDigitos(String cnpj) {
        if (cnpj == null || cnpj.isEmpty()) {
            return "";
        }
        // Rejeita strings que tenham caracteres fora de [0-9 . / -]
        if (!cnpj.matches("[0-9./\\-]+")) {
            return "";
        }
        return cnpj.replaceAll("[^\\d]", "");
    }

    /**
     * Validação completa usada pela API REST (Fase 3): formato E dígitos
     * verificadores. O CLI mantém apenas o formato (paridade com o legado);
     * a API, voltada a dados novos via upload, usa o critério rígido da Receita.
     */
    public static boolean isValidoCompleto(String cnpj) {
        return isFormatoValido(cnpj) && digitosVerificadoresValidos(cnpj);
    }

    /**
     * Validação de módulo 11 dos dígitos verificadores (opcional, para uso futuro).
     * Aceita entrada com ou sem máscara; exige 14 dígitos.
     */
    public static boolean digitosVerificadoresValidos(String cnpj) {
        if (cnpj == null) {
            return false;
        }
        String digits = cnpj.replaceAll("\\D", "");
        if (digits.length() != 14 || digits.matches("^(\\d)\\1{13}$")) {
            return false; // tamanho errado ou todos os dígitos iguais (000...0 etc.)
        }
        return digits.charAt(12) == digitoVerificador(digits, DV1_WEIGHTS)
                && digits.charAt(13) == digitoVerificador(digits, DV2_WEIGHTS);
    }

    /** Pesos oficiais da Receita Federal, aplicados da esquerda para a direita. */
    private static final int[] DV1_WEIGHTS = {5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
    private static final int[] DV2_WEIGHTS = {6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};

    private static char digitoVerificador(String digits, int[] weights) {
        int sum = 0;
        for (int i = 0; i < weights.length; i++) {
            sum += (digits.charAt(i) - '0') * weights[i];
        }
        int remainder = sum % 11;
        return (char) ('0' + (remainder < 2 ? 0 : 11 - remainder));
    }
}
