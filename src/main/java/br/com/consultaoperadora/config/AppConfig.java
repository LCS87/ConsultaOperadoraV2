package br.com.consultaoperadora.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Configuração externa da aplicação (Fase 0 do plano de melhoria).
 *
 * Precedência: variável de ambiente > system property (-Dapp.chave=valor) > application.properties.
 * Substitui os caminhos hardcoded em Windows que existiam no Main.java legado.
 */
public final class AppConfig {

    private static final String DEFAULT_PROPERTIES_FILE = "application.properties";

    private final Path inputFolder;
    private final Path outputFolder;
    private final int threadPoolSize;
    private final List<String> ufsPermitidas;

    private AppConfig(Path inputFolder, Path outputFolder, int threadPoolSize, List<String> ufsPermitidas) {
        this.inputFolder = inputFolder;
        this.outputFolder = outputFolder;
        this.threadPoolSize = threadPoolSize;
        this.ufsPermitidas = List.copyOf(ufsPermitidas);
    }

    public static AppConfig load() {
        Properties fileProps = loadFileProperties();

        String input = resolve("app.input-folder", "./data/export", fileProps);
        String output = resolve("app.output-folder", "./data/arquivos_finalizados", fileProps);
        String poolRaw = resolve("app.thread-pool-size", "6", fileProps);
        String ufsRaw = resolve("app.ufs-permitidas",
                "AL,BA,CE,MA,PB,PE,PI,RN,SE", fileProps);

        int pool;
        try {
            pool = Integer.parseInt(poolRaw.trim());
            if (pool < 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "app.thread-pool-size inválido: '" + poolRaw + "' (esperado inteiro >= 1)", e);
        }

        List<String> ufs = Arrays.stream(ufsRaw.split(","))
                .map(s -> s.trim().toUpperCase())
                .filter(s -> !s.isEmpty())
                .toList();
        if (ufs.isEmpty()) {
            throw new IllegalStateException("app.ufs-permitidas não pode ficar vazia");
        }

        return new AppConfig(Paths.get(input), Paths.get(output), pool, ufs);
    }

    private static Properties loadFileProperties() {
        Properties props = new Properties();
        try (InputStream in = AppConfig.class.getClassLoader()
                .getResourceAsStream(DEFAULT_PROPERTIES_FILE)) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            System.err.println("AVISO: falha ao ler " + DEFAULT_PROPERTIES_FILE + ": " + e.getMessage());
        }
        return props;
    }

    private static String resolve(String key, String defaultValue, Properties fileProps) {
        String envKey = key.toUpperCase().replace('-', '_').replace('.', '_');
        String value = System.getenv(envKey);
        if (value == null || value.isBlank()) {
            value = System.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = fileProps.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            value = defaultValue;
        }
        return value;
    }

    public Path getInputFolder() {
        return inputFolder;
    }

    public Path getOutputFolder() {
        return outputFolder;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public List<String> getUfsPermitidas() {
        return ufsPermitidas;
    }

    @Override
    public String toString() {
        return "AppConfig{input=" + inputFolder + ", output=" + outputFolder
                + ", threads=" + threadPoolSize + ", ufs=" + ufsPermitidas + '}';
    }
}
