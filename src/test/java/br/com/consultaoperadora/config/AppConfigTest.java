package br.com.consultaoperadora.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke tests de configuração (preservados da Fase 0, agora em teste dedicado).
 */
class AppConfigTest {

    @Test
    void configUsaDefaultsQuandoNadaDefinido() {
        System.clearProperty("app.input-folder");
        System.clearProperty("app.output-folder");
        AppConfig cfg = AppConfig.load();
        assertNotNull(cfg.getInputFolder());
        assertNotNull(cfg.getOutputFolder());
        assertTrue(cfg.getThreadPoolSize() >= 1);
        assertTrue(cfg.getUfsPermitidas().contains("AL"));
    }

    @Test
    void configPodeSerSobrepostaPorSystemProperty() {
        System.setProperty("app.input-folder", "/tmp/entrada-teste");
        try {
            AppConfig cfg = AppConfig.load();
            assertEquals("/tmp/entrada-teste", cfg.getInputFolder().toString());
        } finally {
            System.clearProperty("app.input-folder");
        }
    }

    @Test
    void threadPoolInvalidoFalhaComMensagemClara() {
        System.setProperty("app.thread-pool-size", "0");
        try {
            IllegalStateException e = assertThrows(IllegalStateException.class, AppConfig::load);
            assertTrue(e.getMessage().contains("thread-pool-size"));
        } finally {
            System.clearProperty("app.thread-pool-size");
        }
    }
}
