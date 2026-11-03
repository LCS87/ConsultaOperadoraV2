package br.com.consultaoperadora.service;

import br.com.consultaoperadora.model.EmpresaRegistro;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Fase 2 — testes unitários de {@link CsvReaderService} e {@link CsvWriterService}
 * usando @TempDir (sem mocks frágeis de stream).
 */
class CsvIoServicesTest {

    @TempDir
    Path tmp;

    // ---------- CsvReaderService ----------

    @Test
    void leLinhasEmStreamingPulandoCabecalho() throws IOException {
        Path csv = tmp.resolve("in.csv");
        Files.writeString(csv, "h1;h2;h3;h4;h5;h6\na;b;c;d;e;f\ng;h;i;j;k;l\n", StandardCharsets.ISO_8859_1);

        List<String[]> rows = new ArrayList<>();
        long count = new CsvReaderService(StandardCharsets.ISO_8859_1)
                .forEachLine(csv.toFile(), ';', rows::add);

        assertThat(count).isEqualTo(2);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).containsExactly("a", "b", "c", "d", "e", "f");
        assertThat(rows.get(1)).containsExactly("g", "h", "i", "j", "k", "l");
    }

    @Test
    void arquivoComApenasCabecalhoNaoInvocaCallback() throws IOException {
        Path csv = tmp.resolve("so_header.csv");
        Files.writeString(csv, "h1,h2,h3,h4,h5,h6\n", StandardCharsets.ISO_8859_1);

        List<String[]> rows = new ArrayList<>();
        long count = new CsvReaderService(StandardCharsets.ISO_8859_1)
                .forEachLine(csv.toFile(), ',', rows::add);

        assertThat(count).isZero();
        assertThat(rows).isEmpty();
    }

    @Test
    void respeitaCampoEntreAspasComDelimitadorDentro() throws IOException {
        Path csv = tmp.resolve("aspas.csv");
        Files.writeString(csv, "h1;h2;h3;h4;h5;h6\n\"1;2\";x;y;z;w;v\n", StandardCharsets.ISO_8859_1);

        List<String[]> rows = new ArrayList<>();
        new CsvReaderService(StandardCharsets.ISO_8859_1).forEachLine(csv.toFile(), ';', rows::add);

        assertThat(rows.get(0)[0]).isEqualTo("1;2");
    }

    @Test
    void falhaDeValidacaoCsvViraIOExceptionComNomeDoArquivo() throws IOException {
        Path csv = tmp.resolve("quebrado.csv");
        // aspas não fechadas -> CsvValidationException do OpenCSV
        Files.writeString(csv, "h1;h2;h3;h4;h5;h6\n\"aberto;c;d;e;f;g\n", StandardCharsets.ISO_8859_1);

        CsvReaderService reader = new CsvReaderService(StandardCharsets.ISO_8859_1);
        assertThatThrownBy(() -> reader.forEachLine(csv.toFile(), ';', r -> {
        }))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("quebrado.csv");
    }

    // ---------- CsvWriterService ----------

    private static EmpresaRegistro registro(String cnpj, String razao) {
        return new EmpresaRegistro(new String[]{cnpj, razao, "End", "e@x.com", "2020", "82-33111200"});
    }

    @Test
    void exportaComCabecalhoFixoDelimitadorEPontoEVirgulaESemAspas() throws IOException {
        CsvWriterService writer = new CsvWriterService(tmp);
        int generated = writer.export("AL", "CLARO",
                List.of(registro("12345678000199", "Empresa A"), registro("22334455000166", "Empresa B")));

        assertThat(generated).isEqualTo(1);
        Path out = tmp.resolve("AL - CLARO.csv");
        assertThat(out).exists();
        List<String> lines = Files.readAllLines(out, StandardCharsets.ISO_8859_1);
        assertThat(lines.get(0))
                .isEqualTo("cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones");
        assertThat(lines.get(1)).startsWith("12345678000199;Empresa A;");
        assertThat(lines).hasSize(3);
    }

    @Test
    void nomeDoArquivoSeguePadraoLegadoUfEspacoTracoEspacoToken() {
        CsvWriterService writer = new CsvWriterService(tmp);
        writer.export("PE", "SEM_OPERADORA", List.of(registro("12345678000199", "X")));
        assertThat(tmp.resolve("PE - SEM_OPERADORA.csv")).exists();
    }

    @Test
    void listaVaziaNaoGeraArquivoEretornaZero() {
        CsvWriterService writer = new CsvWriterService(tmp);
        assertThat(writer.export("AL", "TIM", List.of())).isZero();
        assertThat(writer.export("AL", "TIM", null)).isZero();
        assertThat(tmp.resolve("AL - TIM.csv")).doesNotExist();
    }

    @Test
    void erroDeIoRetornaZeroSemLancarExcecao() {
        // pasta de saida inexistente -> FileWriter falha -> metodo degrada para 0 (paridade legado)
        CsvWriterService writer = new CsvWriterService(tmp.resolve("nao_existe"));
        assertThat(writer.export("AL", "VIVO", List.of(registro("12345678000199", "X")))).isZero();
    }
}
