package br.com.consultaoperadora.backend.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Linha de registro para consulta paginada (GET /api/v1/registros).
 * Espelha as 6 colunas do export legado + campos derivados da classificação.
 */
@Schema(description = "Registro classificado persistido")
public record RegistroResponse(
        Long id,
        @Schema(example = "12345678000195") String cnpj,
        String razaoSocial,
        String endereco,
        String email,
        String anoAbertura,
        String telefones,
        String telefoneNormalizado,
        @Schema(example = "CLARO") String operadora,
        @Schema(example = "AL") String uf) {
}
