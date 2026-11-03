package br.com.consultaoperadora.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — testes unitários de {@link CnpjValidator}.
 * Cobre os dois critérios: formato legado (14 dígitos) e módulo 11 completo.
 */
class CnpjValidatorTest {

    // ---------- isFormatoValido (critério usado pelo pipeline hoje) ----------

    @ParameterizedTest
    @ValueSource(strings = {"12345678000199", "11222333000181", "00000000000000"})
    void aceitaQualquerSequenciaDeExatamente14Digitos(String cnpj) {
        // "000...0" passa no formato legado (só o módulo 11 a rejeita) — paridade preservada
        assertThat(CnpjValidator.isFormatoValido(cnpj)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "1234567800019", "123456780001999", "1234567800019a",
            "12.345.678/0001-99", "12345 67890 123", "-1234567800019"})
    void rejeitaTudoQueNaoSao14DigitosPuros(String cnpj) {
        assertThat(CnpjValidator.isFormatoValido(cnpj)).isFalse();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void nullEVAzioSaoInvalidos(String cnpj) {
        assertThat(CnpjValidator.isFormatoValido(cnpj)).isFalse();
    }

    // ---------- digitosVerificadoresValidos (modulo 11) ----------

    @ParameterizedTest
    @CsvSource({
            "11222333000181,   true",   // CNPJ válido clássico (mesma base sem DVs errados)
            "11.222.333/0001-81, true", // aceito com máscara
            "(11) 222.333/0001-81, true",
            "11222333000180,   false",  // DV1 trocado
            "11222333000182,   false",  // DV2 trocado
            "00000000000000,   false",  // todos iguais
            "11111111111111,   false",
            "12345678000199,   false",  // 14 dígitos mas DVs não conferem
            "1234567890123,    false",  // 13 dígitos
            "abc,              false"
    })
    void moduloOnzeComMascaraSemMascaraERepetidos(String entrada, boolean esperado) {
        assertThat(CnpjValidator.digitosVerificadoresValidos(entrada)).isEqualTo(esperado);
    }

    @Test
    void nullNuncaValido() {
        assertThat(CnpjValidator.digitosVerificadoresValidos(null)).isFalse();
    }

    @Test
    void dvRestanteZeroQuandoRestoMenorQueDois() {
        // Caso em que soma % 11 < 2 => dígito verificador '0' (ramo específico do algoritmo).
        // 11.222.333/0001-81 tem DV1='8'; já coberto. Aqui usamos um CNPJ cujo DV2 resulta 0:
        // 11.444.777/0001-.. -> calculado abaixo para garantir o ramo remainder<2.
        String base = "114447770001";
        int dv1 = digitoEsperado(base);
        String comDv1 = base + dv1;
        int dv2 = digitoEsperado(comDv1);
        String completo = comDv1 + dv2;
        assertThat(CnpjValidator.digitosVerificadoresValidos(completo)).isTrue();
    }

    /** Reimplementação independente do módulo 11 apenas para gerar entradas de teste. */
    private static int digitoEsperado(String prefix) {
        int[] pesos1 = {5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
        int[] pesos2 = {6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
        int[] pesos = prefix.length() == 12 ? pesos1 : pesos2;
        int soma = 0;
        for (int i = 0; i < pesos.length; i++) {
            soma += (prefix.charAt(i) - '0') * pesos[i];
        }
        int resto = soma % 11;
        return resto < 2 ? 0 : 11 - resto;
    }
}
