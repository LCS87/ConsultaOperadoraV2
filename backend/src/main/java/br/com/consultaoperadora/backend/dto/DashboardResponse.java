package br.com.consultaoperadora.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Resumo geral para o dashboard do frontend (Fase 4 consumira este payload).
 */
@Schema(description = "Resumo da ultima classificacao processada")
public record DashboardResponse(
        @Schema(example = "al_ativos.csv") String fileName,
        String uf,
        long linhasLidas,
        int registrosUnicos,
        List<OperadoraItem> porOperadora) {

    public record OperadoraItem(String operadora, long quantidade) {
    }
}
