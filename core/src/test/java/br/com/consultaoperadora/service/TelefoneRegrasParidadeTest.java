package br.com.consultaoperadora.service;

import org.junit.jupiter.api.Test;

/**
 * Testes de PARIDADE com o legado (Fase 1) — mesma entrada, mesma saída.
 * Casos migrados de MainSmokeTest (Fase 0), agora apontando para os serviços
 * extraídos: TelefoneNormalizer e OperadoraClassifier.
 */
class TelefoneRegrasParidadeTest {

    // ---------- Normalização (Main.normalizeFirstPhoneNumber -> TelefoneNormalizer) ----------

    @Test
    void normalizaFormatoDddHifenNumero() {
        org.junit.jupiter.api.Assertions.assertEquals("558233111200",
                TelefoneNormalizer.normalizeFirstPhoneNumber("82-33111200"));
    }

    @Test
    void normalizaFormatoParenteses() {
        org.junit.jupiter.api.Assertions.assertEquals("558233111200",
                TelefoneNormalizer.normalizeFirstPhoneNumber("(82) 3311-1200"));
    }

    @Test
    void aplicaNonoDigitoParaMobileOitoDigitos() {
        // regex guloso: captura DD + \d{4,5} + \d{4}; 8 digitos iniciados por 6-9 => prefixa 9
        org.junit.jupiter.api.Assertions.assertEquals("5582991111234",
                TelefoneNormalizer.normalizeFirstPhoneNumber("82-9111-1234"));
        org.junit.jupiter.api.Assertions.assertEquals("5582961234567",
                TelefoneNormalizer.normalizeFirstPhoneNumber("82-6123-4567"));   // inicio 6 => mobile
        org.junit.jupiter.api.Assertions.assertEquals("5582991112345",
                TelefoneNormalizer.normalizeFirstPhoneNumber("82-99111-2345"));  // 9 digitos: mantem
    }

    @Test
    void nonoDigitoEmNumeroDeOitoDigitosSemHifenInterno() {
        org.junit.jupiter.api.Assertions.assertEquals("5582991111234",
                TelefoneNormalizer.normalizeFirstPhoneNumber("82-91111234"));
    }

    @Test
    void naoAplicaNonoDigitoParaFixoIniciandoCom3() {
        String norm = TelefoneNormalizer.normalizeFirstPhoneNumber("82-33111200");
        org.junit.jupiter.api.Assertions.assertFalse(norm.startsWith("55829"));
    }

    @Test
    void vazioOuTextoSemNumeroRetornaVazio() {
        org.junit.jupiter.api.Assertions.assertEquals("", TelefoneNormalizer.normalizeFirstPhoneNumber(null));
        org.junit.jupiter.api.Assertions.assertEquals("", TelefoneNormalizer.normalizeFirstPhoneNumber("   "));
        org.junit.jupiter.api.Assertions.assertEquals("", TelefoneNormalizer.normalizeFirstPhoneNumber("sem telefone aqui"));
    }

    @Test
    void paridadeComLegadoDddInvalidoProduzNumeroCurtoEsemOperadora() {
        // Comportamento legado preservado: "(00) ..." casa o segundo ramo do regex e produz
        // um numero curto, que depois e descartado por length < 12 na classificacao.
        org.junit.jupiter.api.Assertions.assertEquals("5500",
                TelefoneNormalizer.normalizeFirstPhoneNumber("(00) numero invalido"));
        org.junit.jupiter.api.Assertions.assertEquals("SEM_OPERADORA",
                OperadoraClassifier.classifyRaw("(00) numero invalido"));
    }

    // ---------- Classificação (Main.getOperadora -> OperadoraClassifier) ----------

    @Test
    void fallbackClassificaPorDigitoIdentificador() {
        org.junit.jupiter.api.Assertions.assertEquals("CLARO", OperadoraClassifier.identifyCarrierFallback("5582712345678")); // digito indice4 = 7 (13 digitos)
        org.junit.jupiter.api.Assertions.assertEquals("VIVO", OperadoraClassifier.identifyCarrierFallback("5582912345678"));   // 9
        org.junit.jupiter.api.Assertions.assertEquals("TIM", OperadoraClassifier.identifyCarrierFallback("5582812345678"));    // 8
        org.junit.jupiter.api.Assertions.assertEquals("OI", OperadoraClassifier.identifyCarrierFallback("5582312345678"));     // 3
        org.junit.jupiter.api.Assertions.assertEquals("SEM_OPERADORA", OperadoraClassifier.identifyCarrierFallback("5582212345678")); // 2 => fora das faixas
        org.junit.jupiter.api.Assertions.assertEquals("SEM_OPERADORA", OperadoraClassifier.identifyCarrierFallback("558231234567"));  // 12 digitos: legado so decide com 13
    }

    @Test
    void getOperadoraSemTelefoneValidoEhSemOperadora() {
        org.junit.jupiter.api.Assertions.assertEquals("SEM_OPERADORA", OperadoraClassifier.classifyRaw(""));
        org.junit.jupiter.api.Assertions.assertEquals("SEM_OPERADORA", OperadoraClassifier.classifyRaw("abcdefg"));
    }
}
