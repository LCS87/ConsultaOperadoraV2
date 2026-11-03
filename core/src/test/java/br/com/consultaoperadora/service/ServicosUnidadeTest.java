package br.com.consultaoperadora.service;

import br.com.consultaoperadora.model.EmpresaRegistro;
import br.com.consultaoperadora.model.Operadora;
import br.com.consultaoperadora.batch.MetricasProcessamento;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests dos novos serviços da Fase 1 (unidades pequenas, sem dependência de disco).
 */
class ServicosUnidadeTest {

    // ---------- CnpjValidator ----------

    @Test
    void validaFormatoDezQuatroDigitos() {
        assertTrue(CnpjValidator.isFormatoValido("12345678000199"));
        assertFalse(CnpjValidator.isFormatoValido("1234567800019"));   // 13 dígitos
        assertFalse(CnpjValidator.isFormatoValido("1234567800019a"));  // não numérico
        assertFalse(CnpjValidator.isFormatoValido(""));
        assertFalse(CnpjValidator.isFormatoValido(null));
    }

    @Test
    void digitosVerificadoresModulo11() {
        // CNPJ real conhecido: 11.222.333/0001-81
        assertTrue(CnpjValidator.digitosVerificadoresValidos("11222333000181"));
        assertTrue(CnpjValidator.digitosVerificadoresValidos("11.222.333/0001-81"));
        assertFalse(CnpjValidator.digitosVerificadoresValidos("11222333000180")); // DV errado
        assertFalse(CnpjValidator.digitosVerificadoresValidos("00000000000000")); // repetido
    }

    // ---------- Operadora enum ----------

    @Test
    void fromRawMapeiaDesconhecidosParaSemOperadora() {
        assertEquals(Operadora.CLARO, Operadora.fromRaw("CLARO"));
        assertEquals(Operadora.SEM_OPERADORA, Operadora.fromRaw("SEM OPERADORA"));
        assertEquals(Operadora.SEM_OPERADORA, Operadora.fromRaw("SERCOMTEL"));
        assertEquals(Operadora.SEM_OPERADORA, Operadora.fromRaw(null));
        assertEquals("SEM_OPERADORA", Operadora.SEM_OPERADORA.fileNameToken());
    }

    // ---------- EmpresaRegistro ----------

    @Test
    void linhaIncompletaViraNull() {
        assertNull(EmpresaRegistro.fromRow(new String[]{"a", "b"}));
        assertNull(EmpresaRegistro.fromRow(null));
    }

    @Test
    void extraiColunasETruncaCnpj() {
        String[] linha = new String[7];
        linha[0] = " 12345678000199 ";
        linha[1] = "Razao";
        linha[2] = "End";
        linha[3] = "e@x";
        linha[4] = "2020";
        linha[5] = "(82) 99111-2345";
        linha[6] = "extra";
        EmpresaRegistro r = EmpresaRegistro.fromRow(linha);
        assertNotNull(r);
        assertEquals("12345678000199", r.cnpj());
        assertEquals("(82) 99111-2345", r.telefonesRaw());
        String[] esperado = new String[6];
        System.arraycopy(linha, 0, esperado, 0, 6);
        assertArrayEquals(esperado, r.asColumns());
    }

    // ---------- CsvDelimiterDetector ----------

    @Test
    void escolhePontoEVirgulaQuandoDaMaisColunas() throws Exception {
        char d = CsvDelimiterDetector.detectFromHeader("a;b;c;d;e;f", "teste.csv");
        assertEquals(';', d);
    }

    @Test
    void escolheTabQuandoNaoHaOutroComMaisColunas() throws Exception {
        char d = CsvDelimiterDetector.detectFromHeader("a\tb\tc\td\te\tf", "teste.csv");
        assertEquals('\t', d);
    }

    @Test
    void cabecalhoNuloLancaIoException() {
        assertThrows(java.io.IOException.class,
                () -> CsvDelimiterDetector.detectFromHeader(null, "vazio.csv"));
    }

    // ---------- DeduplicationService ----------

    @Test
    void dedupAceitaPrimeiroERechaçaDuplicado() {
        DeduplicationService dedup = new DeduplicationService();
        assertTrue(dedup.registerIfNew("12345678000199"));
        assertFalse(dedup.registerIfNew("12345678000199"));
        assertEquals(1, dedup.uniqueCount());
    }

    // ---------- MetricasProcessamento ----------

    @Test
    void contadoresAcumulam() {
        MetricasProcessamento m = new MetricasProcessamento();
        m.addRegistrosUnicos(3);
        m.incrementCsvFilesGerados(2);
        m.markFileSuccess();
        m.markFileFailure();
        assertEquals(3, m.getRegistrosProcessados());
        assertEquals(2, m.getCsvFilesGerados());
        assertEquals(1, m.getSuccessfulFiles());
        assertEquals(1, m.getFailedFiles());
        assertTrue(m.houveFalhas());
    }

    @Test
    void formatacaoPtBrEhHHmmss() {
        assertEquals("1.234", MetricasProcessamento.formatNumber(1234));
        assertEquals("00:01:05", MetricasProcessamento.formatDuration(65));
        assertEquals("01:00:00", MetricasProcessamento.formatDuration(3600));
    }
}
