package br.com.consultaoperadora;

import br.com.consultaoperadora.batch.MetricasProcessamento;
import br.com.consultaoperadora.batch.ProcessamentoBatchService;
import br.com.consultaoperadora.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Bootstrap CLI (Fase 1).
 *
 * Agora apenas inicializa: carrega config, trata argumentos e imprime o
 * relatório final. Todo o pipeline vive em {@link ProcessamentoBatchService}
 * e as regras em serviços dedicados (br.com.consultaoperadora.service.*).
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        AppConfig config;
        try {
            config = AppConfig.load();
        } catch (IllegalStateException e) {
            System.err.println("Configuração inválida: " + e.getMessage());
            System.err.println("Use --help para ver as opções.");
            if (args.length == 0 || !"--help".equals(args[0])) {
                printUsage();
                System.exit(2);
            }
            return;
        }

        if (args.length > 0 && "--help".equals(args[0])) {
            printUsage();
            return;
        }

        // Permite override por argumento posicional: java -jar app.jar <input> [output]
        if (args.length >= 1) {
            config = reloadWithOverrides(config, args);
        }

        log.info("=== INÍCIO DO PROCESSAMENTO === {}", config);

        File inputDir = config.getInputFolder().toFile();
        if (!inputDir.exists()) {
            log.error("Diretório de entrada não encontrado: {}", config.getInputFolder());
            System.exit(1);
            return;
        }

        long startTime = System.nanoTime();
        MetricasProcessamento metricas = new MetricasProcessamento();
        int exitCode = new ProcessamentoBatchService(config, metricas).run(inputDir);
        long durationSec = (System.nanoTime() - startTime) / 1_000_000_000;

        int found = metricas.getSuccessfulFiles() + metricas.getFailedFiles();
        log.info("=== RELATÓRIO FINAL ===");
        log.info("Tempo total: {}", MetricasProcessamento.formatDuration(durationSec));
        log.info("Arquivos processados com sucesso: {}/{}", metricas.getSuccessfulFiles(), found);
        log.info("Registros processados: {}", MetricasProcessamento.formatNumber(metricas.getRegistrosProcessados()));
        log.info("Arquivos CSV gerados: {}", metricas.getCsvFilesGerados());
        if (found > 0) {
            log.info("Taxa de sucesso: {}", String.format("%.1f%%", (metricas.getSuccessfulFiles() * 100.0) / found));
        }
        log.info("=== FIM DO PROCESSAMENTO ===");

        System.exit(exitCode == 0 ? 0 : 1);
    }

    private static AppConfig reloadWithOverrides(AppConfig base, String[] args) {
        System.setProperty("app.input-folder", args[0]);
        if (args.length >= 2) {
            System.setProperty("app.output-folder", args[1]);
        }
        return AppConfig.load();
    }

    private static void printUsage() {
        System.out.println("""
                Uso: java -jar consulta-operadora.jar [INPUT_FOLDER] [OUTPUT_FOLDER]

                Configuração (precedência: variável de ambiente > -Dpropriedade > application.properties):
                  app.input-folder       Pasta com os CSVs de entrada   (var: APP_INPUT_FOLDER)
                  app.output-folder      Pasta de saída dos resultados  (var: APP_OUTPUT_FOLDER)
                  app.thread-pool-size   Threads paralelas (default 6)  (var: APP_THREAD_POOL_SIZE)
                  app.ufs-permitidas     UFs aceitas, separadas por vírgula (var: APP_UFS_PERMITIDAS)
                """);
    }
}
