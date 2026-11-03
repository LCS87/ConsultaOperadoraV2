package br.com.consultaoperadora.backend.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Testes de integracao da API REST (Fase 3): sobem o contexto Spring completo
 * e exercitam os endpoints com MockMvc, incluindo Swagger/OpenAPI.
 */
@SpringBootTest
@AutoConfigureMockMvc
class ApiIntegracaoTest {

    @Autowired
    private MockMvc mockMvc;

    private static final String CSV_OK = """
            cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
            12345678000199;Alpha Ltda;Rua A;a@a.com;2010;82-3311-1200
            12345678000199;Duplicada;Rua X;d@d.com;2010;82-3311-1200
            INVALIDOCNPJ;Sem CNPJ;Rua Y;e@e.com;2011;82-3311-1200
            """;

    @Test
    void uploadCsvValidoRetornaClassificacao() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "al_ativos.csv", "text/csv",
                CSV_OK.getBytes(StandardCharsets.ISO_8859_1));

        mockMvc.perform(multipart("/api/classificacao").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.uf").value("AL"))
                .andExpect(jsonPath("$.linhasLidas").value(3))
                .andExpect(jsonPath("$.registrosUnicos").value(1))
                .andExpect(jsonPath("$.contagemPorOperadora.FIXO").value(1));
    }

    @Test
    void uploadNomeSemUfRetorna400ComMensagemJson() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "sp_ativos.csv", "text/csv",
                CSV_OK.getBytes(StandardCharsets.ISO_8859_1));

        mockMvc.perform(multipart("/api/classificacao").file(file))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.mensagem", containsString("UF")));
    }

    @Test
    void uploadArquivoNaoCsvEhRejeitado() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "nota.txt", "text/plain", "oi".getBytes());

        mockMvc.perform(multipart("/api/classificacao").file(file))
                .andExpect(status().isBadRequest());
    }

    @Test
    void validarCnpjEndpoint() throws Exception {
        mockMvc.perform(get("/api/cnpj/validar").param("cnpj", "12.345.678/0001-95"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.formatoValido").value(true))
                .andExpect(jsonPath("$.valido").value(true));

        mockMvc.perform(get("/api/cnpj/validar").param("cnpj", "11111111111111"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valido").value(false));
    }

    @Test
    void classificarTelefoneEndpoint() throws Exception {
        mockMvc.perform(get("/api/telefone/classificar").param("telefones", "82-3311-1200"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.operadoraAgregada").value(containsString("FIXO")));
    }

    @Test
    void dashboardResumoRetornaContratoVazio() throws Exception {
        mockMvc.perform(get("/api/dashboard/resumo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.porOperadora").isArray());
    }

    @Test
    void openApiSpecDisponivel() throws Exception {
        MvcResult result = mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andReturn();
        String json = result.getResponse().getContentAsString();
        assertThat(json)
                .contains("/api/classificacao")
                .contains("/api/cnpj/validar")
                .contains("Consulta Operadora API");
    }
}
