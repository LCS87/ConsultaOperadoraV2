package br.com.consultaoperadora.backend.service;

import br.com.consultaoperadora.backend.entity.ProcessamentoJobEntity;
import br.com.consultaoperadora.backend.repository.DuplicidadeLogRepository;
import br.com.consultaoperadora.backend.repository.EmpresaRegistroRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testes de integração da FASE 4 (Persistência JPA/Hibernate + Flyway).
 *
 * Cobrem exatamente o critério de aceite da fase:
 *   1. reprocessar o mesmo arquivo NÃO duplica CNPJs (dedupe vem do banco);
 *   2. todo descarte é auditado em duplicidade_log com o motivo correto;
 *   3. métricas do job (linhas, inseridos, duplicados, inválidos) são persistentes;
 *   4. endpoints /api/v1 (upload, registros, export, dashboard) operam sobre o banco.
 */
@SpringBootTest
@AutoConfigureMockMvc
class IngestaoPersistenciaIntegracaoTest {

    /**
     * CSV com 6 linhas de dados:
     *  - ...0001-95 FIXO (82-3311-1200) — válida
     *  - ...0001-81 VIVO móvel (82-99111-2200) — válida
     *  - repetição exata da primeira linha -> DUPLICADO_INTERNO
     *  - CNPJ com DV inválido -> descartado como inválido
     *  - repetição exata da segunda linha -> DUPLICADO_INTERNO
     *  - terceira empresa válida, telefone sem DDD reconhecível -> SEM_OPERADORA
     */
    private static final String CSV_BASE = """
            cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
            12.345.678/0001-95;Alpha Ltda;Rua A;a@a.com;2010;82-3311-1200
            11.222.333/0001-81;Beta SA;Av. B;b@b.com;2015;82-99111-2200
            12.345.678/0001-95;Alpha Dup;Rua A2;a2@a.com;2010;82-3311-1200
            12.345.678/0001-90;CNPJ Invalido DV;Rua C;c@c.com;2011;82-3311-1200
            11.222.333/0001-81;Beta Dup;Av. B2;b2@b.com;2015;82-99111-2200
            99.888.777/0001-00;Gamma Ltda;Rua D;d@d.com;2019;+55 99 91234-5678
            """;

    @Autowired
    private IngestaoService ingestao;

    @Autowired
    private EmpresaRegistroRepository registros;

    @Autowired
    private DuplicidadeLogRepository duplicidades;

    @Autowired
    private MockMvc mockMvc;

    private Path escreverCsvTemporario(String nome, String conteudo) throws Exception {
        Path tmp = Files.createTempFile("ingestao-", ".csv");
        Path renomeado = tmp.resolveSibling(nome);
        Files.writeString(tmp, conteudo, StandardCharsets.ISO_8859_1);
        Files.move(tmp, renomeado);
        renomeado.toFile().deleteOnExit();
        return renomeado;
    }

    @Test
    void primeiraIngestaoPersisteRegistrosEAuditaDuplicadosInternos() throws Exception {
        long baseRegistros = registros.count();
        long baseLogs = duplicidades.count();

        ProcessamentoJobEntity job = ingestao.processar(
                escreverCsvTemporario("al_ativos.csv", CSV_BASE));

        assertThat(job.getStatus().name()).isEqualTo("CONCLUIDO");
        assertThat(job.getLinhasLidas()).isEqualTo(6);
        // válidos únicos por formato+DV: ...0001-95, ...0001-81, 99.888.777/0001-25 => 3
        assertThat(job.getRegistrosInseridos()).isEqualTo(3);
        assertThat(registros.count()).isEqualTo(baseRegistros + 3);
        // 2 repetições internas no arquivo auditadas
        assertThat(duplicidades.count()).isEqualTo(baseLogs + 2);
        assertThat(duplicidades.findAll().stream()
                .filter(l -> l.getJobId().equals(job.getId()))
                .allMatch(l -> "DUPLICADO_INTERNO".equals(l.getMotivo()))).isTrue();
        assertThat(job.getCnpjInvalidosDescartados()).isEqualTo(1);
        assertThat(job.getPorOperadora()).contains("FIXO").contains("VIVO");
    }

    @Test
    void reprocessarOMesmoArquivoNaoDuplicaCnpjs_criterioDeAceite() throws Exception {
        Path csv = escreverCsvTemporario("al_reprocessa.csv", CSV_BASE);

        ingestao.processar(csv);
        long aposPrimeiro = registros.count();

        ProcessamentoJobEntity segundo = ingestao.processar(csv);

        assertThat(segundo.getStatus().name()).isEqualTo("CONCLUIDO");
        assertThat(registros.count()).isEqualTo(aposPrimeiro); // NENHUM registro novo
        assertThat(segundo.getRegistrosInseridos()).isZero();
        // os 3 únicos caem como JA_PERSISTIDO + 2 internos = 5 descartes
        assertThat(segundo.getDuplicadosDescartados()).isEqualTo(5);
        assertThat(duplicidades.findAll().stream()
                .filter(l -> l.getJobId().equals(segundo.getId()))
                .filter(l -> "CNPJ_JA_PERSISTIDO".equals(l.getMotivo())))
                .hasSize(3);
    }

    @Test
    void nomeSemUfPermitidaFalhaAntesDeGravarQualquerCoisa() throws Exception {
        Path csv = escreverCsvTemporario("sp_ativos.csv", CSV_BASE);

        long antes = registros.count();
        assertThatThrownBy(() -> ingestao.processar(csv))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UF");
        assertThat(registros.count()).isEqualTo(antes);
    }

    @Test
    void cicloHttpUploadV1DepoisConsultaExportEDashboard() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "al_ciclo_http_" + System.nanoTime() + ".csv", "text/csv",
                CSV_BASE.getBytes(StandardCharsets.ISO_8859_1));

        mockMvc.perform(multipart("/api/v1/arquivos/upload").file(file))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.uf").value("AL"))
                .andExpect(jsonPath("$.status").value("CONCLUIDO"))
                .andExpect(jsonPath("$.registrosInseridos").value(3));

        // Consulta paginada filtrada por UF
        mockMvc.perform(get("/api/v1/registros").param("uf", "AL").param("size", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElementos").isNumber());

        // Busca por operadora inválida -> 400 com mensagem clara
        mockMvc.perform(get("/api/v1/registros").param("operadora", "FOOOPERADORA"))
                .andExpect(status().isBadRequest());

        // Dashboard reflete contagens reais do banco
        mockMvc.perform(get("/api/v1/dashboard/resumo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRegistros").isNumber())
                .andExpect(jsonPath("$.itens").isArray());

        // Export CSV direto do banco: header + linhas, UTF-8 com BOM
        var exportResult = mockMvc.perform(get("/api/v1/registros/export").param("uf", "AL"))
                .andExpect(status().isOk())
                .andReturn();
        String corpo = new String(exportResult.getResponse().getContentAsByteArray(),
                StandardCharsets.UTF_8);
        assertThat(corpo).startsWith("\uFEFF"); // BOM
        assertThat(corpo).contains("cnpj_completo,razao_social");
        assertThat(corpo.lines().count()).isGreaterThanOrEqualTo(2);
    }
}
