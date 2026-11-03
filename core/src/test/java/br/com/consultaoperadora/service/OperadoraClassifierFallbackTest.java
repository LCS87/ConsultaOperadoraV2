package br.com.consultaoperadora.service;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Fase 2 — ramos de {@link OperadoraClassifier} que dependem do estado global da
 * libphonenumber (base de metadados/carriers) e por isso não são alcançáveis de
 * forma determinística com números reais em CI: carrier desconhecido, mapper
 * vazio/nulo, falha do mapper, tipos não-móveis, número inválido e parse com erro.
 * Mockamos apenas o <em>ambiente externo</em> (biblioteca), nunca a regra de
 * negócio sob teste.
 *
 * ATENÇÃO aos números usados: o fallback legado decide pelo dígito no ÍNDICE 4 do
 * E.164 ("55" + DDD + ...). Em "5582991112345" o índice 4 é '9' => VIVO.
 * Nos ramos de fallback usamos o formato DD-NNNNN-NNNN (ex.: "82-99111-2345"),
 * pois o padrão do normalizador exige separadores para capturar 5+4 dígitos;
 * sem separadores, a gulodice do regex (\d{4,5} seguido de \d{4}) tritura o
 * nono dígito e o E.164 resultante tem outro dígito na posição 4.
 */
class OperadoraClassifierFallbackTest {

    @Test
    void carrierDesconhecidoEhSanitizadoParaUppercaseSemSimbolos() throws Exception {
        // "SERCOMTEL LTDA." -> remove não-letras -> "SERCOMTELLTDA" (comportamento legado);
        // o enum fromRaw converte desconhecidos em SEM_OPERADORA, como antes.
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.MOBILE, "SERCOMTEL LTDA.", null)) {
            String raw = OperadoraClassifier.classifyRaw("5582991112345");
            assertThat(raw).isEqualTo("SERCOMTELLTDA");
            assertThat(br.com.consultaoperadora.model.Operadora.fromRaw(raw))
                    .isEqualTo(br.com.consultaoperadora.model.Operadora.SEM_OPERADORA);
        }
    }

    @Test
    void carrierVazioCaiNoFallbackPorDigitoIdentificador() throws Exception {
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.MOBILE, "", null)) {
            // normaliza para 5582991112345 -> índice4='9' -> VIVO
            assertThat(OperadoraClassifier.classifyRaw("82-99111-2345")).isEqualTo("VIVO");
        }
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.MOBILE, null, null)) {
            // normaliza para 5582891112345 -> índice4='8' -> TIM
            assertThat(OperadoraClassifier.classifyRaw("82-89111-2345")).isEqualTo("TIM");
        }
    }

    @Test
    void falhaDoCarrierMapperCaiNoFallback() throws Exception {
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.FIXED_LINE_OR_MOBILE,
                null, new IllegalStateException("base corrompida"))) {
            assertThat(OperadoraClassifier.classifyRaw("82-99111-2345")).isEqualTo("VIVO");
        }
    }

    @Test
    void tipoNaoMovelENaoFixoViraSemOperadora() throws Exception {
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.VOIP, null, null)) {
            assertThat(OperadoraClassifier.classifyRaw("5582991112345")).isEqualTo("SEM_OPERADORA");
        }
    }

    @Test
    void numeroInvalidoViraSemOperadora() throws Exception {
        try (Fixture f = new Fixture(false, PhoneNumberUtil.PhoneNumberType.MOBILE, null, null)) {
            assertThat(OperadoraClassifier.classifyRaw("5582991112345")).isEqualTo("SEM_OPERADORA");
        }
    }

    @Test
    void parseInvalidoLancaNumberParseEHTratadoComoSemOperadora() throws Exception {
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.MOBILE, null, null)) {
            when(f.utilMock.parse(any(), any())).thenThrow(
                    new NumberParseException(NumberParseException.ErrorType.NOT_A_NUMBER, "x"));
            // "(82) 3311-1200" normaliza para 558233111200 (>=12), mas o parse explode.
            assertThat(OperadoraClassifier.classifyRaw("(82) 3311-1200")).isEqualTo("SEM_OPERADORA");
        }
    }

    @Test
    void excecaoGlobalDoPipelineDeClassificacaoUsaFallbackDePrefixo() throws Exception {
        try (Fixture f = new Fixture(true, PhoneNumberUtil.PhoneNumberType.MOBILE, null, null)) {
            when(f.utilMock.getNumberType(any())).thenThrow(new RuntimeException("falha interna"));
            // ramo catch (Exception) -> identifyCarrierFallback: índice4='9' -> VIVO
            assertThat(OperadoraClassifier.classifyRaw("82-99111-2345")).isEqualTo("VIVO");
        }
    }

    // ---------- fixture de mocks estáticos (um registro por vez por thread) ----------

    /** Registra os mocks estáticos de PhoneNumberUtil/CarrierMapper e fecha na ordem inversa. */
    private static final class Fixture implements AutoCloseable {
        final PhoneNumberUtil utilMock;
        private final MockedStatic<PhoneNumberUtil> utilStatic;
        private final MockedStatic<PhoneNumberToCarrierMapper> mapperStatic;

        Fixture(boolean valid, PhoneNumberUtil.PhoneNumberType type,
                String carrierName, RuntimeException mapperFailure) {
            utilMock = mock(PhoneNumberUtil.class);
            when(utilMock.isValidNumber(any())).thenReturn(valid);
            when(utilMock.getNumberType(any())).thenReturn(type);
            utilStatic = Mockito.mockStatic(PhoneNumberUtil.class);
            utilStatic.when(PhoneNumberUtil::getInstance).thenReturn(utilMock);

            var mapperMock = mock(PhoneNumberToCarrierMapper.class);
            if (mapperFailure != null) {
                when(mapperMock.getNameForNumber(any(), any())).thenThrow(mapperFailure);
            } else {
                when(mapperMock.getNameForNumber(any(), any())).thenReturn(carrierName);
            }
            mapperStatic = Mockito.mockStatic(PhoneNumberToCarrierMapper.class);
            mapperStatic.when(PhoneNumberToCarrierMapper::getInstance).thenReturn(mapperMock);
        }

        @Override
        public void close() {
            mapperStatic.close();
            utilStatic.close();
        }
    }
}
