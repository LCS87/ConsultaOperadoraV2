package br.com.consultaoperadora.backend.api;

import br.com.consultaoperadora.backend.dto.DashboardResponse;
import br.com.consultaoperadora.service.CnpjValidator;
import br.com.consultaoperadora.service.OperadoraClassifier;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Endpoints de consulta leve (Fase 3): validacao de CNPJ, classificacao de telefone
 * isolada e resumo para dashboard. Servem de contrato para o frontend Angular (Fase 4).
 */
@RestController
@RequestMapping("/api")
@Tag(name = "Consultas", description = "Validacao de CNPJ e classificacao pontual de telefones")
public class ConsultaController {

    @Operation(summary = "Valida formato + digitos verificadores de um CNPJ")
    @GetMapping("/cnpj/validar")
    public Map<String, Object> validarCnpj(@Parameter(example = "12.345.678/0001-95") @RequestParam String cnpj) {
        boolean formato = CnpjValidator.isFormatoValido(cnpj);
        return Map.of(
                "cnpj", cnpj == null ? "" : cnpj,
                "formatoValido", formato,
                "valido", CnpjValidator.isValidoCompleto(cnpj));
    }

    @Operation(summary = "Classifica um conjunto de telefones (separados por virgula) na operadora")
    @GetMapping("/telefone/classificar")
    public ResponseEntity<Map<String, Object>> classificarTelefone(@RequestParam String telefones) {
        if (telefones == null || telefones.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("erro", "Informe 'telefones'."));
        }
        List<String> classificacoes = Arrays.stream(telefones.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(t -> t + " -> " + OperadoraClassifier.classifyRaw(t))
                .toList();
        return ResponseEntity.ok(Map.of(
                "telefonesRaw", telefones,
                "classificacoes", classificacoes,
                "operadoraAgregada", OperadoraClassifier.classifyRaw(telefones)));
    }

    @Operation(summary = "Resumo agregado da ultima classificacao (placeholder do dashboard - Fase 4)")
    @GetMapping("/dashboard/resumo")
    public DashboardResponse resumo() {
        // Fase 3: payload de contrato; dados persistidos chegam na Fase 5 (Spring Data JPA).
        return new DashboardResponse(null, null, 0, 0, List.of());
    }
}
