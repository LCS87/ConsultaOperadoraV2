package br.com.consultaoperadora.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — testes unitários de {@link TelefoneNormalizer} cobrindo todos os
 * formatos suportados, a regra do nono dígito e as gulodices do regex legado
 * (protegidas por paridade, não "corrigidas").
 */
class TelefoneNormalizerTest {

    @ParameterizedTest
    @CsvSource({
            "'82-33111200',        558233111200",   // DD-hifen, 8 dígitos colados
            "'(82) 3311-1200',     558233111200",   // formato (DD) com máscara
            "'82 3311 1200',       558233111200",   // espaços como separadores
            "'82-9111-1234',       5582991111234",  // 4+4 iniciando em 9 -> ganha nono dígito
            "'(82) 99111-2345',    5582991112345",  // 9 dígitos já com nono
            "'5582991112345',      555582991112345" // colado: DD '55' capturado, resto vira número (paridade)
    })
    void formatosConhecidosNormalizamComoNoLegado(String entrada, String esperado) {
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber(entrada)).isEqualTo(esperado);
    }

    @ParameterizedTest
    @ValueSource(strings = {"82-6111-1234", "82-7111-1234", "82-8111-1234", "82-9111-1234"})
    void oitavaInicialEntreSeisENoveRecebeNonoDigito(String entrada) {
        String saida = TelefoneNormalizer.normalizeFirstPhoneNumber(entrada);
        assertThat(saida).startsWith("55829");
        assertThat(saida).hasSize(13);
    }

    @ParameterizedTest
    @ValueSource(strings = {"82-3111-1234", "82-5111-1234"})
    void oitavaInicialAbaixoDeSeisNaoRecebeNonoDigito(String entrada) {
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber(entrada)).hasSize(12);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "( ) 99111-2345", "82-()1234", "sem numero", "1234567",
            "55 82 9 9111 1234"}) // regex legado exige 4 dígitos após o DDD; aqui só há 1
    void entradasSemTelefoneValidoViramStringVazia(String entrada) {
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber(entrada)).isEmpty();
    }

    @Test
    void nullETratadoComoVazio() {
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber(null)).isEmpty();
    }

    @Test
    void pegaApenasOPrimeiroNumeroQuandoHaListaSeparadaPorVirgula() {
        // Paridade com o legado: regex acha o PRIMEIRO match e ignora o resto.
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber("82-33111200, 82-991112345"))
                .isEqualTo("558233111200");
    }

    @Test
    void gulodiceDoRegexCapturaQuatroECincoDigitosNaOrdemCerta() {
        // \d{4,5} guloso come 5 dígitos quando possível: 82-991112345 -> grupos 99111 + 2345
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber("82-991112345"))
                .isEqualTo("5582991112345");
    }

    @Test
    void dddInvalidoDoisZeroEDuasLetrasPassaPelaNormalizacaoMasNaoPeloClassifier() {
        // Normalização legada NÃO valida DDD: '(00) 3311-1200' produz '550033111200'.
        // A rejeição acontece na classificação (isValidNumber == false).
        assertThat(TelefoneNormalizer.normalizeFirstPhoneNumber("(00) 3311-1200"))
                .isEqualTo("550033111200");
        assertThat(OperadoraClassifier.classifyRaw("(00) 3311-1200"))
                .isEqualTo("SEM OPERADORA");
    }
}
