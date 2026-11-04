package br.com.consultaoperadora.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalização de telefones brasileiros (Fase 1) — extraído de
 * {@code Main.normalizeFirstPhoneNumber}.
 *
 * Regras preservadas exatamente como no legado:
 *  - texto contendo "()" ou vazio => resultado vazio;
 *  - dois formatos reconhecidos: "DD-NNNNN NN" e "(DD) números";
 *  - regra do nono dígito: 8 dígitos iniciados por 6-9 recebem prefixo "9";
 *  - saída no formato E.164 sem "+": "55" + DDD + número.
 */
public final class TelefoneNormalizer {

    /** Mesmo padrão do legado (inclui a gulodice de \d{4,5} que os testes protegem). */
    private static final Pattern FLEX_NUMBER_PATTERN = Pattern.compile(
            "(\\d{2})[- ]*(\\d{4,5})[- ]*(\\d{4})|\\((\\d{2})\\)\\s*([\\d\\s-]+)");

    private TelefoneNormalizer() {
    }

    /** @return número normalizado ("55DDDNUMERO") ou "" quando não reconhecido. */
    public static String normalizeFirstPhoneNumber(String telefonesRaw) {
        if (telefonesRaw == null || telefonesRaw.trim().isEmpty() || telefonesRaw.contains("()")) {
            return "";
        }
        Matcher matcher = FLEX_NUMBER_PATTERN.matcher(telefonesRaw);

        String ddd = null;
        String numero = null;

        if (matcher.find()) {
            if (matcher.group(1) != null) {          // formato DD-NNNNNNNN (ex: 82-33111200)
                ddd = matcher.group(1);
                numero = matcher.group(2) + matcher.group(3);
            } else if (matcher.group(4) != null) {   // formato (DD) NNNN-NNNN
                ddd = matcher.group(4);
                numero = matcher.group(5).replaceAll("[^\\d]", "");
            }
        }
        if (ddd == null || numero == null) {
            return "";
        }
        numero = numero.replaceAll("[^\\d]", "");

        // Regra do nono dígito: 8 dígitos começando com 6-9 => prefixa 9
        if (numero.length() == 8 && numero.matches("^[6-9]\\d{7}$")) {
            numero = "9" + numero;
        }
        return "55" + ddd + numero;
    }
}
