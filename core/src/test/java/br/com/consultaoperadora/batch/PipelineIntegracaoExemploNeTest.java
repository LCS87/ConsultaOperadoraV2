package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.config.AppConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — teste de integração do pipeline sobre o CSV de exemplo versionado em
 * src/test/resources/exemplo_ne/, conferindo as saídas por operadora e as métricas.
 */
class PipelineIntegracaoExemploNeTest {

    @TempDir
    Path outputDir;

    private AppConfig configPara(Path input) {
        System.setProperty("app.input-folder", input.toString());
        System.setProperty("app.output-folder", outputDir.toString());
        return AppConfig.load();
    }

    @Test
    void rodaPipelineSobreCsvDeExemploEConfereSaidasPorOperadora() throws IOException {
        Path input = Path.of("src/test/resources/exemplo_ne");
        AppConfig cfg = configPara(input);
        try {
            MetricasProcessamento metricas = new MetricasProcessamento();
            int exitCode = new ProcessamentoBatchService(cfg, metricas).run(input.toFile());

            assertThat(exitCode).isZero();
            // linhas unicas validas: Alpha (fixo), Beta (movel), Gamma (sem fone) => 3
            // ("44..." CNPJ invalido e "555...77" linha incompleta sao descartadas)
            assertThat(metricas.getRegistrosProcessados()).isEqualTo(3);
            assertThat(metricas.getSuccessfulFiles()).isEqualTo(1);
            assertThat(metricas.getFailedFiles()).isZero();

            Path fixo = outputDir.resolve("AL - FIXO.csv");
            assertThat(fixo).exists();
            List<String> linhasFixo = Files.readAllLines(fixo);
            assertThat(linhasFixo.get(0)).isEqualTo(
                    "cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones");
            assertThat(linhasFixo).hasSize(2); // cabecalho + Alpha (duplicata fora)
            assertThat(linhasFixo.get(1)).contains("Alpha Telecom Ltda");

            Path semOp = outputDir.resolve("AL - SEM_OPERADORA.csv");
            assertThat(semOp).exists();
            assertThat(Files.readAllLines(semOp)).anyMatch(l -> l.contains("Gamma Sem Fone"));

            // duplicata do mesmo CNPJ exportada apenas uma vez no arquivo da operadora
            assertThat(linhasFixo.stream().filter(l -> l.startsWith("11222333000181")).count())
                    .isEqualTo(1);
        } finally {
            System.clearProperty("app.input-folder");
            System.clearProperty("app.output-folder");
        }
    }
}
