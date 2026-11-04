package br.com.consultaoperadora.backend.api;

import br.com.consultaoperadora.backend.dto.DashboardV2Response;
import br.com.consultaoperadora.backend.dto.JobResponse;
import br.com.consultaoperadora.backend.dto.RegistroResponse;
import br.com.consultaoperadora.model.EmpresaRegistro;
import br.com.consultaoperadora.backend.entity.ProcessamentoJobEntity;
import br.com.consultaoperadora.backend.service.IngestaoService;
import br.com.consultaoperadora.backend.service.UploadProcessingService;
import br.com.consultaoperadora.model.Operadora;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * API v1 com persistência (Fase 4). Versão de URL `/api/v1` conforme plano;
 * os endpoints legados da Fase 3 continuam ativos até o frontend (Fase 6)
 * consumir apenas esta superfície.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Pipeline v1", description = "Upload, processamento, consulta e dashboard backed pelo banco (JPA/Hibernate)")
public class PipelineV1Controller {

    private final UploadProcessingService uploads;
    private final IngestaoService ingestao;
    private final br.com.consultaoperadora.backend.repository.EmpresaRegistroRepository registrosRepo;
    private final br.com.consultaoperadora.backend.repository.ProcessamentoJobRepository jobsRepo;

    public PipelineV1Controller(UploadProcessingService uploads,
                                IngestaoService ingestao,
                                br.com.consultaoperadora.backend.repository.EmpresaRegistroRepository registrosRepo,
                                br.com.consultaoperadora.backend.repository.ProcessamentoJobRepository jobsRepo) {
        this.uploads = uploads;
        this.ingestao = ingestao;
        this.registrosRepo = registrosRepo;
        this.jobsRepo = jobsRepo;
    }

    @Operation(summary = "Upload + ingestão de um CSV no banco (dedupe persistente por CNPJ)")
    @PostMapping(value = "/arquivos/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<JobResponse> upload(@RequestPart("file") MultipartFile file) throws IOException {
        ProcessamentoJobEntity job;
        try {
            // ingestao.processar lanca IOException (verificado) -> embalada em UncheckedIOException
            // e desembalada no catch abaixo.
            job = uploads.processarComPersistencia(file, path -> {
                try {
                    return ingestao.processar(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(toJobResponse(job));
    }

    @Operation(summary = "Últimos jobs de processamento (status + métricas)")
    @GetMapping("/processamentos")
    public List<JobResponse> listarJobs() {
        return jobsRepo.findTop20ByOrderByIdDesc().stream().map(PipelineV1Controller::toJobResponse).toList();
    }

    @Operation(summary = "Status/métricas de um job específico")
    @GetMapping("/processamentos/{id}")
    public JobResponse obterJob(@PathVariable Long id) {
        return toJobResponse(jobsRepo.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Job não encontrado: " + id)));
    }

    @Operation(summary = "Busca paginada e filtrável dos registros classificados")
    @GetMapping("/registros")
    public Map<String, Object> buscarRegistros(
            @Parameter(example = "CLARO") @RequestParam(required = false) String operadora,
            @Parameter(example = "AL") @RequestParam(required = false) String uf,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        String op = null;
        if (operadora != null && !operadora.isBlank()) {
            try {
                op = Operadora.valueOf(operadora.trim().toUpperCase(java.util.Locale.ROOT)).name();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Operadora inválida: '" + operadora
                        + "'. Valores aceitos: " + java.util.Arrays.toString(Operadora.values()));
            }
        }
        String ufNorm = (uf == null || uf.isBlank()) ? null : uf.trim().toUpperCase(java.util.Locale.ROOT);
        Page<EmpresaRegistro> resultado = registrosRepo.buscar(op, ufNorm,
                PageRequest.of(Math.max(page, 0), Math.min(Math.max(size, 1), 200),
                        Sort.by(Sort.Direction.ASC, "id")));
        return Map.of(
                "conteudo", resultado.getContent().stream().map(PipelineV1Controller::toRegistroResponse).toList(),
                "pagina", resultado.getNumber(),
                "tamanhoPagina", resultado.getSize(),
                "totalElementos", resultado.getTotalElements(),
                "totalPaginas", resultado.getTotalPages());
    }

    @Operation(summary = "Download CSV (UTF-8 com BOM) do filtro atual — lido direto do banco")
    @GetMapping("/registros/export")
    public ResponseEntity<StreamingResponseBody> export(
            @RequestParam(required = false) String operadora,
            @RequestParam(required = false) String uf) {
        String op = (operadora == null || operadora.isBlank())
                ? null : Operadora.valueOf(operadora.trim().toUpperCase(java.util.Locale.ROOT)).name();
        String ufNorm = (uf == null || uf.isBlank()) ? null : uf.trim().toUpperCase(java.util.Locale.ROOT);

        StreamingResponseBody body = out -> {
            out.write(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}); // BOM p/ Excel
            Writer w = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            w.write("cnpj_completo,razao_social,endereco_completo,email,ano_abertura,telefones,operadora,uf\n");
            int pagina = 0;
            Page<EmpresaRegistro> lote;
            do {
                lote = registrosRepo.buscar(op, ufNorm, PageRequest.of(pagina++, 500));
                for (EmpresaRegistro e : lote) {
                    w.write(String.join(",",
                            csv(e.cnpj()), csv(e.razaoSocial()), csv(e.enderecoCompleto()),
                            csv(e.email()), csv(e.anoAbertura()), csv(e.telefonesRaw()),
                            e.getOperadora(), e.getUf()));
                    w.write("\n");
                }
                w.flush();
            } while (lote.hasNext());
        };
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"registros_export.csv\"")
                .contentType(MediaType.parseMediaType("text/csv; charset=UTF-8"))
                .body(body);
    }

    @Operation(summary = "Contagens reais por operadora/UF para gráficos do frontend")
    @GetMapping("/dashboard/resumo")
    public DashboardV2Response resumo() {
        IngestaoService.DashboardSnapshot s = ingestao.dashboard();
        return new DashboardV2Response(s.totalRegistros(), s.totalJobs(),
                s.itens().stream()
                        .map(i -> new DashboardV2Response.OperadoraUfItem(i.operadora(), i.uf(), i.quantidade()))
                        .toList());
    }

    static JobResponse toJobResponse(ProcessamentoJobEntity j) {
        var dur = j.duracao();
        return new JobResponse(j.getId(), j.getArquivo().getNomeArquivo(), j.getArquivo().getUf(),
                j.getStatus().name(), j.getLinhasLidas(), j.getRegistrosInseridos(),
                j.getDuplicadosDescartados(), j.getCnpjInvalidosDescartados(), j.getPorOperadora(),
                j.getIniciadoEm(), j.getFinalizadoEm(), dur == null ? null : dur.toMillis(),
                j.getMensagemErro());
    }

    static RegistroResponse toRegistroResponse(EmpresaRegistro e) {
        return new RegistroResponse(e.getId(), e.cnpj(), e.razaoSocial(), e.enderecoCompleto(),
                e.email(), e.anoAbertura(), e.telefonesRaw(), e.getTelefoneNormalizado(),
                e.getOperadora(), e.getUf());
    }

    private static String csv(String valor) {
        if (valor == null) {
            return "";
        }
        boolean precisaAspas = valor.contains(",") || valor.contains("\"")
                || valor.startsWith(" ") || valor.endsWith(" ");
        String escapado = valor.replace("\"", "\"\"");
        return precisaAspas ? "\"" + escapado + "\"" : escapado;
    }
}
