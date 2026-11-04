package br.com.consultaoperadora.backend.api;

import br.com.consultaoperadora.batch.MotorClassificacaoService;
import br.com.consultaoperadora.backend.dto.ClassificacaoResponse;
import br.com.consultaoperadora.backend.service.UploadProcessingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * POST /api/classificacao — upload de CSV e classificacao on demand (Fase 3).
 */
@RestController
@RequestMapping("/api")
@Tag(name = "Classificacao", description = "Classifica um arquivo CSV de export por operadora")
public class ClassificacaoController {

    private final UploadProcessingService uploadService;

    public ClassificacaoController(UploadProcessingService uploadService) {
        this.uploadService = uploadService;
    }

    @Operation(summary = "Faz upload de um CSV e retorna a classificacao por operadora",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Classificado com sucesso"),
                    @ApiResponse(responseCode = "400", description = "Arquivo invalido ou UF nao permitida",
                            content = @Content)
            })
    @PostMapping(value = "/classificacao", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ClassificacaoResponse classificar(@RequestPart("file") MultipartFile file) {
        MotorClassificacaoService.Resultado r = null;
        try {
            r = uploadService.processarUpload(file);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Falha ao ler o CSV enviado: " + e.getMessage(), e);
        }
        return toResponse(r);
    }

    static ClassificacaoResponse toResponse(MotorClassificacaoService.Resultado r) {
        return new ClassificacaoResponse(r.fileName(), r.uf(), r.linhasLidas(),
                r.registrosUnicos(), r.contagemPorOperadora(), r.detalhePorOperadora());
    }
}
