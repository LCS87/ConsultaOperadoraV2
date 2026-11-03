package br.com.consultaoperadora;

import br.com.consultaoperadora.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke tests da Fase 0: garantem que config externa funciona e
 * que as regras movidas do Main legado preservam o comportamento.
 */
class MainSmokeTest {

    // ---------- AppConfig ----------

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

    // ---------- Regras de telefone (paridade com legado) ----------

    @Test
    void normalizaFormatoDddHifenNumero() {
        assertEquals("558233111200", Main.normalizeFirstPhoneNumber("82-33111200"));
    }

    @Test
    void normalizaFormatoParenteses() {
        assertEquals("558233111200", Main.normalizeFirstPhoneNumber("(82) 3311-1200"));
    }

    @Test
    void aplicaNonoDigitoParaMobileOitoDigitos() {
        // regex guloso: captura DD + \d{4,5} + \d{4}; 8 digitos iniciados por 6-9 => prefixa 9
        assertEquals("5582991111234", Main.normalizeFirstPhoneNumber("82-9111-1234"));
        assertEquals("5582961234567", Main.normalizeFirstPhoneNumber("82-6123-4567"));   // inicio 6 => mobile
        assertEquals("5582991112345", Main.normalizeFirstPhoneNumber("82-99111-2345"));  // 9 digitos: mantem
    }

    @Test
    void nonoDigitoEmNumeroDeOitoDigitosSemHifenInterno() {
        assertEquals("5582991111234", Main.normalizeFirstPhoneNumber("82-91111234"));
    }

    @Test
    void naoAplicaNonoDigitoParaFixoIniciandoCom3() {
        String norm = Main.normalizeFirstPhoneNumber("82-33111200");
        assertFalse(norm.startsWith("55829"));
    }

    @Test
    void vazioOuTextoSemNumeroRetornaVazio() {
        assertEquals("", Main.normalizeFirstPhoneNumber(null));
        assertEquals("", Main.normalizeFirstPhoneNumber("   "));
        assertEquals("", Main.normalizeFirstPhoneNumber("sem telefone aqui"));
    }

    @Test
    void paridadeComLegadoDddInvalidoProduzNumeroCurtoEsemOperadora() {
        // Comportamento legado preservado: "(00) ..." casa o segundo ramo do regex e produz
        // um numero curto, que depois e descartado por length < 12 no getOperadora().
        assertEquals("5500", Main.normalizeFirstPhoneNumber("(00) numero invalido"));
        assertEquals("SEM OPERADORA", Main.getOperadora("(00) numero invalido"));
    }

    @Test
    void fallbackClassificaPorDigitoIdentificador() {
        assertEquals("CLARO", Main.identifyCarrierFallback("5582712345678")); // digito indice4 = 7 (13 digitos)
        assertEquals("VIVO", Main.identifyCarrierFallback("5582912345678"));   // 9
        assertEquals("TIM", Main.identifyCarrierFallback("5582812345678"));    // 8
        assertEquals("OI", Main.identifyCarrierFallback("5582312345678"));     // 3
        assertEquals("SEM OPERADORA", Main.identifyCarrierFallback("5582212345678")); // 2 => fora das faixas
        assertEquals("SEM OPERADORA", Main.identifyCarrierFallback("558231234567"));  // 12 digitos: legado so decide com 13
    }

    @Test
    void getOperadoraSemTelefoneValidoEhSemOperadora() {
        assertEquals("SEM OPERADORA", Main.getOperadora(""));
        assertEquals("SEM OPERADORA", Main.getOperadora("abcdefg"));
    }

    // ---------- Filtro/selecao de UF via config ----------

    @Test
    void isMatchingCsvReconhecePadroesDeNome() throws Exception {
        AppConfig cfg = AppConfig.load();
        Main app = new Main(cfg);
        assertTrue(app.isMatchingCsv("al_ativos.csv"));
        assertTrue(app.isMatchingCsv("ativos_se.csv"));
        assertFalse(app.isMatchingCsv("sp_ativos.csv"));
        assertFalse(app.isMatchingCsv("al_ativos.txt"));
    }

    @Test
    void ufsConfiguraveisAlteramODomínio() throws Exception {
        System.setProperty("app.ufs-permitidas", "SP,RJ");
        try {
            AppConfig cfg = AppConfig.load();
            List<String> ufs = cfg.getUfsPermitidas();
            assertEquals(List.of("SP", "RJ"), ufs);
            Main app = new Main(cfg);
            assertTrue(app.isMatchingCsv("ativos_sp.csv"));
            assertFalse(app.isMatchingCsv("ativos_al.csv"));
        } finally {
            System.clearProperty("app.ufs-permitidas");
        }
    }
}
