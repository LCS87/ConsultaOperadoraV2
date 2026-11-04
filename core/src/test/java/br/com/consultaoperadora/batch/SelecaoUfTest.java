package br.com.consultaoperadora.batch;

import br.com.consultaoperadora.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Testes de seleção/extração de UF (preservados da Fase 0, agora no serviço de batch).
 */
class SelecaoUfTest {

    @Test
    void isMatchingCsvReconhecePadroesDeNome() {
        AppConfig cfg = AppConfig.load();
        ProcessamentoBatchService svc = new ProcessamentoBatchService(cfg, new MetricasProcessamento());
        assertTrue(svc.isMatchingCsv("al_ativos.csv"));
        assertTrue(svc.isMatchingCsv("ativos_se.csv"));
        assertFalse(svc.isMatchingCsv("sp_ativos.csv"));
        assertFalse(svc.isMatchingCsv("al_ativos.txt"));
    }

    @Test
    void ufsConfiguraveisAlteramODominio() {
        System.setProperty("app.ufs-permitidas", "SP,RJ");
        try {
            AppConfig cfg = AppConfig.load();
            List<String> ufs = cfg.getUfsPermitidas();
            assertEquals(List.of("SP", "RJ"), ufs);
            ProcessamentoBatchService svc = new ProcessamentoBatchService(cfg, new MetricasProcessamento());
            assertTrue(svc.isMatchingCsv("ativos_sp.csv"));
            assertFalse(svc.isMatchingCsv("ativos_al.csv"));
        } finally {
            System.clearProperty("app.ufs-permitidas");
        }
    }

    @Test
    void extraiUfDePrefixoESufixo() {
        AppConfig cfg = AppConfig.load(); // inclui AL..SE
        ProcessamentoBatchService svc = new ProcessamentoBatchService(cfg, new MetricasProcessamento());
        assertEquals("AL", svc.extractStateFromFileName("al_ativos.csv"));
        assertEquals("PE", svc.extractStateFromFileName("exportacao_pe.csv"));
        assertEquals("XX", svc.extractStateFromFileName("sp_ativos.csv"));
    }
}
