package br.com.consultaoperadora.batch;

import java.text.NumberFormat;
import java.util.Locale;
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
