package br.com.consultaoperadora.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

/**
 * Resposta do endpoint de classificacao (Fase 3).
 * Espelha {@code MotorClassificacaoService.Resultado} em formato JSON amigavel.
 */
@Schema(description = "Resultado da classificacao de um arquivo CSV")
public record ClassificacaoResponse(
        @Schema(example = "al_ativos.csv") String fileName,
        @Schema(example = "AL") String uf,
        @Schema(description = "Total de linhas lidas no CSV") long linhasLidas,
        @Schema(description = "Registros unicos validos apos filtro de CNPJ e deduplicacao") int registrosUnicos,
        @Schema(description = "Contagem por operadora (token -> quantidade)") Map<String, Long> contagemPorOperadora,
        @Schema(description = "Registros detalhados por operadora (6 colunas do export)") Map<String, List<String[]>> detalhePorOperadora) {
}
