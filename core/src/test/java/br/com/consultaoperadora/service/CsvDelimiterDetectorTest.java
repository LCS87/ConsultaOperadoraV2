package br.com.consultaoperadora.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fase 2 — testes unitários de {@link CsvDelimiterDetector}.
 */
class CsvDelimiterDetectorTest {

    @TempDir
    Path tmp;

    @Test
    void escolheVirgulaQuandoElaGeraMaisColunas() throws IOException {
        assertThat(CsvDelimiterDetector.detectFromHeader("a,b,c,d,e,f", "t.csv")).isEqualTo(',');
    }

    @Test
    void escolhePontoEVirgulaQuandoElaGeraMaisColunas() throws IOException {
        assertThat(CsvDelimiterDetector.detectFromHeader("a;b;c;d;e;f", "t.csv")).isEqualTo(';');
    }

    @Test
    void escolheTabQuandoNenhumOutroTemMaisColunas() throws IOException {
        assertThat(CsvDelimiterDetector.detectFromHeader("a\tb\tc\td\te\tf", "t.csv")).isEqualTo('\t');
    }

    @Test
    void venceEmpatePelaOrdemDeTentativaVirgulaPrimeiro() throws IOException {
        // mesmo nº de colunas para ',' e ';' -> mantém o primeiro candidato (',')
        assertThat(CsvDelimiterDetector.detectFromHeader("a,b;c,d", "t.csv")).isEqualTo(',');
    }

    @Test
    void cabecalhoComMenosColunasQueOEsperadoAvisaMasNaoFalha() throws IOException {
        // maxColumns=3 < EXPECTED_MIN_COLUMNS=6 -> log.warn e melhor palpite ';'?
        // Não: ';' gera 3 colunas, ',' e TAB geram 1. O melhor continua ';'.
        assertThat(CsvDelimiterDetector.detectFromHeader("a;b;c", "t.csv")).isEqualTo(';');
    }

    @Test
    void linhaNulaLancaIOExceptionComMensagemClara() {
        assertThatThrownBy(() -> CsvDelimiterDetector.detectFromHeader(null, "vazio.csv"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Arquivo vazio");
    }

    @Test
    void detectLeOPrimeiroBytesDoArquivoComEncodingInformado() throws IOException {
        Path csv = tmp.resolve("real.csv");
        Files.writeString(csv, "cnpj;razao;end;email;ano;fone\n1;2;3;4;5;6\n", StandardCharsets.ISO_8859_1);
        assertThat(CsvDelimiterDetector.detect(csv.toFile(), StandardCharsets.ISO_8859_1)).isEqualTo(';');
    }

    @Test
    void arquivoTotalmenteVazioFalhaComoIoException() throws IOException {
        Path csv = tmp.resolve("vazio.csv");
        Files.createFile(csv);
        assertThatThrownBy(() -> CsvDelimiterDetector.detect(csv.toFile(), StandardCharsets.ISO_8859_1))
                .isInstanceOf(IOException.class);
    }
}
