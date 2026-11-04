package br.com.consultaoperadora.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Resumo de um job de processamento (GET /api/v1/processamentos/{id}).
 */
@Schema(description = "Status e métricas acumuladas de um job")
public record JobResponse(
        Long id,
        String arquivo,
        String uf,
        String status,
        long linhasLidas,
        int registrosInseridos,
        int duplicadosDescartados,
        int cnpjInvalidosDescartados,
        String porOperadora,
        Instant iniciadoEm,
        Instant finalizadoEm,
        Long duracaoMs,
        String mensagemErro) {
}
