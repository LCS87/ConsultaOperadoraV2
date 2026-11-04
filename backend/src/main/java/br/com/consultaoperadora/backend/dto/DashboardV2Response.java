package br.com.consultaoperadora.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

/**
 * Payload do POST /api/v1/processamentos e GET /api/v1/dashboard/resumo.
 */
@Schema(description = "Resumo agregado por operadora/UF para gráficos e upload")
public record DashboardV2Response(
        long totalRegistros,
        long totalJobs,
        List<OperadoraUfItem> itens) {

    @Schema(description = "Contagem por par (operadora, uf)")
    public record OperadoraUfItem(String operadora, String uf, long quantidade) {
    }
}
