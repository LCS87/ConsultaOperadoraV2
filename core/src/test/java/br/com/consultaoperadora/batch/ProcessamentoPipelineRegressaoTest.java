package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.config.AppConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Teste de regressão E2E da Fase 1: roda o pipeline refatorado sobre um CSV de
 * exemplo e verifica os arquivos de saída {UF} - {OPERADORA}.csv, métricas e
 * contadores — o mesmo cenário validado manualmente na Fase 0.
 */
class ProcessamentoPipelineRegressaoTest {

    @TempDir
    Path inputDir;

    @TempDir
    Path outputDir;

    @Test
    void pipelineGeraArquivosPorOperadoraComoLegado() throws IOException {
        String csv = """
                cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
                12345678000199;Empresa Valida Ltda;Rua A, 100;a@x.com;2010;82-33111200
                12345678000199;Duplicada;Rua B, 200;b@x.com;2011;82-33111200
                12;CNPJ Invalido;Rua C;;2012;(82) 99111-2345
                22334455000166;Moveis Claro;Rua D, 40;d@x.com;2015;82-9111-1234
                33445566000177;Sem Telefone;Rua E, 50;e@x.com;2016;
                44556677000188;Linha Curta;Rua F;f@x.com;2017;(00) invalido
                55667788;Linha Incompleta
                """;
        Files.writeString(inputDir.resolve("al_ativos.csv"), csv, StandardCharsets.ISO_8859_1);

        System.setProperty("app.input-folder", inputDir.toString());
        System.setProperty("app.output-folder", outputDir.toString());
        try {
            AppConfig cfg = AppConfig.load();
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(inputDir.toFile());

            assertEquals(0, exitCode, "pipeline não pode ter falhas");
            // linhas válidas únicas: Empresa Valida (FIXO), Moveis Claro, Sem Telefone, Linha Curta => 4
            assertEquals(4, metricas.getRegistrosProcessados());
            assertEquals(metricas.getSuccessfulFiles(), 1);
            assertEquals(0, metricas.getFailedFiles());

            Map<String, String> esperados = Map.of(
                    "AL - FIXO.csv", "Empresa Valida",
                    "AL - SEM_OPERADORA.csv", "Sem Telefone");
            for (Map.Entry<String, String> e : esperados.entrySet()) {
                Path out = outputDir.resolve(e.getKey());
                assertTrue(Files.exists(out), "esperado arquivo " + e.getKey());
                List<String> lines = Files.readAllLines(out, StandardCharsets.ISO_8859_1);
                assertEquals("cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones",
                        lines.get(0), "cabeçalho idêntico ao legado em " + e.getKey());
                assertTrue(lines.stream().anyMatch(l -> l.contains(e.getValue())),
                        e.getKey() + " deveria conter " + e.getValue());
            }

            // duplicata descartada: apenas 1 linha com o CNPJ 12345678000199 no total exportado
            long countFixo = Files.readAllLines(outputDir.resolve("AL - FIXO.csv"), StandardCharsets.ISO_8859_1)
                    .stream().filter(l -> l.startsWith("12345678000199")).count();
            assertEquals(1, countFixo);

            assertTrue(metricas.getCsvFilesGerados() >= 2, "pelo menos FIXO e SEM_OPERADORA gerados");
        } finally {
            System.clearProperty("app.input-folder");
            System.clearProperty("app.output-folder");
        }
    }

    @Test
    void pastaSemCsvDeUfPermitidaRetornaZeroSemArquivos() throws IOException {
        Files.writeString(inputDir.resolve("sp_ativos.csv"), "a,b,c,d,e,f\n", StandardCharsets.ISO_8859_1);
        System.setProperty("app.input-folder", inputDir.toString());
        System.setProperty("app.output-folder", outputDir.toString());
        try {
            AppConfig cfg = AppConfig.load();
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(inputDir.toFile());
            assertEquals(0, exitCode);
            assertEquals(0, metricas.getCsvFilesGerados());
        } finally {
            System.clearProperty("app.input-folder");
            System.clearProperty("app.output-folder");
        }
    }
}
