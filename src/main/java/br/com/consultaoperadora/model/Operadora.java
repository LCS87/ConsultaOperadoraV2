package br.com.consultaoperadora.model;

import java.util.Locale;

/**
 * Operadoras reconhecidas pelo pipeline (Fase 1 do plano de melhoria).
 *
 * Substitui as strings mágicas "CLARO", "VIVO", ... espalhadas pelo Main legado.
 * {@link #fileNameToken()} preserva exatamente o nome de arquivo gerado pelo
 * código original: "{UF} - {OPERADORA}.csv" (com SEM_OPERADORA usando underscore,
 * como no legado).
 */
public enum Operadora {
    CLARO("CLARO"),
    VIVO("VIVO"),
    TIM("TIM"),
    OI("OI"),
    FIXO("FIXO"),
    SEM_OPERADORA("SEM OPERADORA");

    private final String displayName;

    Operadora(String displayName) {
        this.displayName = displayName;
    }

    /** Nome humano usado em logs (ex.: "SEM OPERADORA"). */
    public String displayName() {
        return displayName;
    }

    /** Token usado no nome do arquivo de saída (ex.: "SEM_OPERADORA"). */
    public String fileNameToken() {
        return name();
    }

    /**
     * Mapeia o resultado bruto de classificação (String do legado ou carrier
     * mapper) para o enum. Qualquer valor desconhecido vira SEM_OPERADORA —
     * comportamento idêntico ao switch default do Main atual.
     */
    public static Operadora fromRaw(String raw) {
        if (raw == null) {
            return SEM_OPERADORA;
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        for (Operadora o : values()) {
            if (o.name().equals(normalized) || o.displayName.equals(normalized)) {
                return o;
            }
        }
        return SEM_OPERADORA;
    }
}
