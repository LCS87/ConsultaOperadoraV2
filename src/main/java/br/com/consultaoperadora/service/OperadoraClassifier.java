package br.com.consultaoperadora.service;

import br.com.consultaoperadora.model.Operadora;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberToCarrierMapper;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberType;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Classificação de operadora (Fase 1) — extraído de {@code Main.getOperadora},
 * {@code identifyCarrier} e {@code identifyCarrierFallback}.
 *
 * Estratégia idêntica ao legado:
 *  1. normaliza o telefone ({@link TelefoneNormalizer});
 *  2. consulta tipo + carrier via libphonenumber (+ mapa de carriers pt-BR);
 *  3. FIXO puro => FIXO; MOBILE / FIXED_LINE_OR_MOBILE => carrier mapeado;
 *  4. qualquer falha/valor desconhecido => fallback por dígito identificador
 *     (posição 5 do número E.164 de 13 dígitos) ou SEM_OPERADORA.
 */
public final class OperadoraClassifier {

    private static final Logger log = LoggerFactory.getLogger(OperadoraClassifier.class);

    private OperadoraClassifier() {
    }

    public static Operadora classify(String telefonesRaw) {
        return Operadora.fromRaw(classifyRaw(telefonesRaw));
    }

    /**
     * Versão que devolve a String bruta do legado (útil para logs de diagnóstico e
     * para paridade exata com {@code Main.getOperadora}).
     */
    public static String classifyRaw(String telefonesRaw) {
        String fullNumber = TelefoneNormalizer.normalizeFirstPhoneNumber(telefonesRaw);
        if (fullNumber.isEmpty() || fullNumber.length() < 12) {
            return Operadora.SEM_OPERADORA.displayName();
        }
        try {
            PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
            PhoneNumber numberProto = phoneUtil.parse(fullNumber, "BR");
            if (!phoneUtil.isValidNumber(numberProto)) {
                return Operadora.SEM_OPERADORA.displayName();
            }
            PhoneNumberType type = phoneUtil.getNumberType(numberProto);
            if (type == PhoneNumberType.FIXED_LINE) {
                return Operadora.FIXO.name();
            }
            if (type == PhoneNumberType.MOBILE || type == PhoneNumberType.FIXED_LINE_OR_MOBILE) {
                return identifyCarrier(numberProto, fullNumber);
            }
            return Operadora.SEM_OPERADORA.displayName();
        } catch (NumberParseException e) {
            return Operadora.SEM_OPERADORA.displayName();
        } catch (Exception e) {
            // Comportamento legado: erro inesperado no mapper cai no fallback de prefixo
            return identifyCarrierFallback(fullNumber);
        }
    }

    static String identifyCarrier(PhoneNumber numberProto, String fullNumber) {
        try {
            PhoneNumberToCarrierMapper carrierMapper = PhoneNumberToCarrierMapper.getInstance();
            String carrier = carrierMapper.getNameForNumber(numberProto, Locale.forLanguageTag("pt-BR"));
            if (carrier != null && !carrier.isEmpty()) {
                String upperCarrier = carrier.toUpperCase(Locale.ROOT);
                if (upperCarrier.contains("CLARO")) return Operadora.CLARO.name();
                if (upperCarrier.contains("VIVO")) return Operadora.VIVO.name();
                if (upperCarrier.contains("TIM")) return Operadora.TIM.name();
                if (upperCarrier.contains("OI")) return Operadora.OI.name();
                return upperCarrier.replaceAll("[^A-Z]", "");
            }
        } catch (Exception e) {
            log.debug("Carrier mapper falhou, usando fallback", e);
        }
        return identifyCarrierFallback(fullNumber);
    }

    /** Fallback histórico por dígito identificador (posições do número de 13 dígitos). */
    public static String identifyCarrierFallback(String fullNumber) {
        if (fullNumber.length() < 12) {
            return Operadora.SEM_OPERADORA.displayName();
        }
        if (fullNumber.length() == 13) {
            String digitoIdentificador = fullNumber.substring(4, 5);
            return switch (digitoIdentificador) {
                case "6", "7" -> Operadora.CLARO.name();
                case "9" -> Operadora.VIVO.name();
                case "8" -> Operadora.TIM.name();
                case "3" -> Operadora.OI.name();
                default -> Operadora.SEM_OPERADORA.displayName();
            };
        }
        return Operadora.SEM_OPERADORA.displayName();
    }
}
