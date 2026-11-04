package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.config.AppConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — ramos de erro do orquestrador: falha de detecção de delimitador e
 * falha na preparação da pasta de saída, ambos medindo as métricas corretamente
 * sem derrubar o run. (Arquivos reais em @TempDir — determinístico.)
 */
class ProcessamentoRamosDeFalhaTest {

    @TempDir
    Path tmp;

    private AppConfig configPara(Path input, Path output) {
        System.setProperty("app.input-folder", input.toString());
        System.setProperty("app.output-folder", output.toString());
        return AppConfig.load();
    }

    private void clearProps() {
        System.clearProperty("app.input-folder");
        System.clearProperty("app.output-folder");
    }

    @Test
    void arquivoVazioSemLinhaDeCabecalhoMarcaFalhaEExitCodeUm() throws IOException {
        Path input = Files.createDirectories(tmp.resolve("in"));
        Files.createFile(input.resolve("al_ativos.csv")); // 0 bytes -> detect lança IOException
        Path output = tmp.resolve("out");

        AppConfig cfg = configPara(input, output);
        try {
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(input.toFile());

            assertThat(exitCode).isEqualTo(1);
            assertThat(metricas.getFailedFiles()).isEqualTo(1);
            assertThat(metricas.getSuccessfulFiles()).isZero();
            assertThat(metricas.houveFalhas()).isTrue();
        } finally {
            clearProps();
        }
    }

    @Test
    void pastaDeEntradaInexistenteListaNuloEProcedeGraficamente() throws IOException {
        Path input = tmp.resolve("nao_existe");
        Path output = tmp.resolve("out");
        AppConfig cfg = configPara(input, output);
        try {
            MetricasProcessamento metricas = new MetricasProcessamento();
            // listFiles retorna null -> comportamento legado: warn + 0, sem exceção
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(input.toFile());
            assertThat(exitCode).isZero();
            assertThat(metricas.getCsvFilesGerados()).isZero();
        } finally {
            clearProps();
        }
    }

    @Test
    void pastaDeSaidaOcupadaPorArquivoRetornaUm() throws IOException {
        // Cria um ARQUIVO no caminho que deveria ser a pasta de saída -> createDirectories falha
        Path blockedOutput = tmp.resolve("bloqueado");
        Files.writeString(blockedOutput, "sou arquivo, nao pasta");
        Path input = Files.createDirectories(tmp.resolve("in2"));
        Files.writeString(input.resolve("ce_ativos.csv"), "a;b;c;d;e;f\n1;2;3;4;5;6\n");

        AppConfig cfg = configPara(input, blockedOutput);
        try {
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(input.toFile());
            assertThat(exitCode).isEqualTo(1); // nao conseguiu preparar a pasta de saida
        } finally {
            clearProps();
        }
    }

    @Test
    void processaMultiplosArquivosEmParaleloComMetricasConsolidadas() throws IOException {
        Path input = Files.createDirectories(tmp.resolve("multi"));
        String csv = """
                cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
                11222333000181;Alpha AL;Rua A;a@x.com;2010;82-33111200
                """;
        Files.writeString(input.resolve("al_ativos.csv"), csv);
        Files.writeString(input.resolve("ba_ativos.csv"), csv.replace("82-", "71-"));
        Files.writeString(input.resolve("sp_ativos.csv"), csv); // UF nao permitida: ignorado

        Path output = tmp.resolve("out");
        AppConfig cfg = configPara(input, output);
        try {
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(input.toFile());

            assertThat(exitCode).isZero();
            assertThat(metricas.getSuccessfulFiles()).isEqualTo(2);
            assertThat(metricas.getRegistrosProcessados()).isEqualTo(2);
            assertThat(output.resolve("AL - FIXO.csv")).exists();
            assertThat(output.resolve("BA - FIXO.csv")).exists();
            assertThat(output.resolve("SP - FIXO.csv")).doesNotExist();
        } finally {
            clearProps();
        }
    }
}
