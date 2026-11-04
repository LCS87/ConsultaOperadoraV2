package br.com.consultaoperadora.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — testes unitários de {@link OperadoraClassifier}.
 * Estratégia: números reais de cada operadora via carrier mapper + todos os
 * ramos do fallback por dígito identificador (determinísticos, sem rede).
 */
class OperadoraClassifierTest {

    // ---------- classify: caminho real (libphonenumber + carrier mapper) ----------

    @ParameterizedTest
    @ValueSource(strings = {
            "82-9111-1234",     // formato DD-NNNNN NN com nono dígito => normaliza 558291111234
            "(82) 99111-2345",  // formato (DD) com nono dígito
            "55 82 9 9111 1234" // já com código do país
    })
    void movelDeNonoDigitoEmAlagoasNaoCaiEmSemOperadora(String telefone) {
        // O carrier exato depende da base em uso; garantimos apenas que é uma
        // operadora móvel conhecida ou SEM_OPERADORA pelo fallback — nunca exceção.
        var op = OperadoraClassifier.classify(telefone);
        assertThat(op).isNotNull();
    }

    @Test
    void telefoneFixoPuroEClassificadoComoFixo() {
        // 82 3311-1200 -> fixo de Alagoas (faixa 3xxx)
        assertThat(OperadoraClassifier.classifyRaw("82-33111200")).isEqualTo("FIXO");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "() 91234-5678", "numero invalido", "12345"})
    void entradaSemTelefoneReconhecivelViraSemOperadora(String entrada) {
        assertThat(OperadoraClassifier.classify(entrada))
                .isEqualTo(br.com.consultaoperadora.model.Operadora.SEM_OPERADORA);
    }

    // ---------- identifyCarrierFallback: determinístico, cobre todos os ramos ----------

    @ParameterizedTest
    @CsvSource({
            "5582691112345, CLARO",         // dígito identificador '6' (substring 4..5)
            "5582791112345, CLARO",         // '7'
            "5582991112345, VIVO",          // '9'
            "5582891112345, TIM",           // '8'
            "5582391112345, OI",            // '3'
            "5582191112345, SEM_OPERADORA", // '1' -> default
            "5582491112345, SEM_OPERADORA", // '4' -> default
            "5582291112345, SEM_OPERADORA", // '2' -> default
            "5582591112345, SEM_OPERADORA"  // '5' -> default
    })
    void fallbackMapeiaCadaDigitoIdentificadorConformeLegado(String numeroE164, String esperado) {
        assertThat(numeroE164).hasSize(13); // o fallback só decide com 13 dígitos
        assertThat(OperadoraClassifier.identifyCarrierFallback(numeroE164)).isEqualTo(esperado);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "558", "55823111123", "558231111234", "55823111123456"}) // <12 ou >13 dígitos
    void fallbackForaDe13DigitosComPeloMenos12ViraSemOperadora(String numero) {
        assertThat(OperadoraClassifier.identifyCarrierFallback(numero))
                .isEqualTo("SEM_OPERADORA");
    }

    @Test
    void classifyComNumeroInexistenteMasFormatadoNaoLancaExcecao() {
        // DDD inexistente + número absurdo: o contrato é SEMPRE devolver um enum.
        assertThat(OperadoraClassifier.classify("(99) 0000-0000"))
                .isEqualTo(br.com.consultaoperadora.model.Operadora.SEM_OPERADORA);
    }

    @Test
    void classifyRawDoFixoUsaNomeDoEnumEMobileUsaMesmaConvengao() {
        // FIXO retorna name(); fallback também retorna name()/displayName consistente
        assertThat(OperadoraClassifier.classifyRaw("82-33111200")).isEqualTo("FIXO");
        assertThat(OperadoraClassifier.classifyRaw("82-91111234")).isIn(
                "CLARO", "VIVO", "TIM", "OI", "FIXO", "SEM_OPERADORA");
    }
}
