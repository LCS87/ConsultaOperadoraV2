package br.com.consultaoperadora.batch;

import java.text.NumberFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Contadores atômicos do processamento (Fase 1) — extraídos dos estáticos
 * globais de {@code Main}. Uma instância por execução; segura para threads.
 */
public final class MetricasProcessamento {

    private static final Locale LOCALE_PT_BR = new Locale("pt", "BR");

    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final AtomicInteger totalCsvFilesGenerated = new AtomicInteger(0);
    private final AtomicInteger successfulFiles = new AtomicInteger(0);
    private final AtomicInteger failedFiles = new AtomicInteger(0);

    public void addRegistrosUnicos(int count) {
        totalRecordsProcessed.addAndGet(count);
    }

    public void incrementCsvFilesGerados(int count) {
        totalCsvFilesGenerated.addAndGet(count);
    }

    public void markFileSuccess() {
        successfulFiles.incrementAndGet();
    }

    public void markFileFailure() {
        failedFiles.incrementAndGet();
    }

    public long getRegistrosProcessados() {
        return totalRecordsProcessed.get();
    }

    public int getCsvFilesGerados() {
        return totalCsvFilesGenerated.get();
    }

    public int getSuccessfulFiles() {
        return successfulFiles.get();
    }

    public int getFailedFiles() {
        return failedFiles.get();
    }

    public boolean houveFalhas() {
        return failedFiles.get() > 0;
    }

    // ------------------------------------------------------------------
    // Métricas por operadora (Fase 3) — usadas pelo dashboard da API REST.
    // Aditivas: não alteram os contadores globais nem o toString legado.
    // ------------------------------------------------------------------

    private final ConcurrentHashMap<String, AtomicLong> porOperadora = new ConcurrentHashMap<>();

    /** Conta um registro classificado em uma operadora (token do enum, ex.: CLARO). */
    public void addRegistroOperadora(String operadoraToken, long count) {
        if (operadoraToken == null || count <= 0) {
            return;
        }
        porOperadora.computeIfAbsent(operadoraToken, k -> new AtomicLong()).addAndGet(count);
    }

    /** Cópia snapshot das contagens por operadora (ordem de inserção preservada). */
    public Map<String, Long> getContagemPorOperadora() {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        porOperadora.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    /** Formata número no padrão pt-BR (milhar com ponto), como nos logs originais. */
    public static String formatNumber(long value) {
        return NumberFormat.getNumberInstance(LOCALE_PT_BR).format(value);
    }

    /** Formata segundos como HH:mm:ss, idêntico ao Main legado. */
    public static String formatDuration(long totalSeconds) {
        long h = totalSeconds / 3600;
        long m = (totalSeconds % 3600) / 60;
        long s = totalSeconds % 60;
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    @Override
    public String toString() {
        int found = successfulFiles.get() + failedFiles.get();
        return "Metricas{registros=" + formatNumber(totalRecordsProcessed.get())
                + ", csvGerados=" + totalCsvFilesGenerated.get()
                + ", ok=" + successfulFiles.get() + "/" + found + '}';
    }
}
