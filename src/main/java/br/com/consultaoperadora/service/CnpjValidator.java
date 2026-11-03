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

    /** Critério usado pelo pipeline hoje: não vazio + exatamente 14 dígitos. */
    public static boolean isFormatoValido(String cnpj) {
        return cnpj != null && !cnpj.isEmpty() && DEZ_QUATRO_DIGITOS.matcher(cnpj).matches();
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
        return dvValido(digits, 0, 12, 5) && dvValido(digits, 0, 13, 6);
    }

    private static boolean dvValido(String digits, int start, int length, int weightStart) {
        int sum = 0;
        int weight = weightStart;
        for (int i = start + length - 1; i >= start; i--) {
            sum += (digits.charAt(i) - '0') * weight;
            weight = weight == 2 ? 9 : weight - 1;
        }
        int remainder = sum % 11;
        int expected = remainder < 2 ? 0 : 11 - remainder;
        return digits.charAt(length) == (char) ('0' + expected);
    }
}
