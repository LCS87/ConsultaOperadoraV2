package br.com.consultaoperadora.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testes do MotorClassificacaoService (Fase 3) — o motor sob demanda usado
 * pela API REST para classificar arquivos de upload sem tocar no pipeline CLI.
 */
class MotorClassificacaoTest {

    private static final byte[] CSV_AL = ("""
            cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
            11222333000181;Alpha Telecom Ltda;Rua A, 100 - Centro;alpha@x.com;2010;82-33111200
            11222333000181;Alpha duplicada;Rua A, 100;b@x.com;2010;82-33111200
            22333444000181;Beta Movel SA;Rua B, 200;c@x.com;2015;82-9111-1234
            99999999999999;DV Inválido;Rua C;;2019;82-33222333
            """).getBytes(StandardCharsets.ISO_8859_1);

    @Test
    void classificaArquivoValidoComDuplicadosEInvalidos(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("al_ativos.csv");
        Files.write(csv, CSV_AL);

        MotorClassificacaoService.Resultado r =
                new MotorClassificacaoService(List.of("AL")).classificarArquivo(csv);

        assertThat(r.uf()).isEqualTo("AL");
        assertThat(r.linhasLidas()).isEqualTo(4);
        // API usa validação completa: 1 duplicado + 1 CNPJ com DV inválido descartados => 2 únicos
        assertThat(r.registrosUnicos()).isEqualTo(2);
        assertThat(r.contagemPorOperadora())
                .containsEntry("FIXO", 1L)      // 82-33111200
                .containsEntry("CLARO", 1L);    // 82-9111-1234 -> nono dígito -> 82991111234
        assertThat(r.detalhePorOperadora()).hasSize(2);
        assertThat(r.detalhePorOperadora().get("FIXO"))
                .anySatisfy(col -> assertThat(col).contains("Alpha Telecom"));
    }

    @Test
    void detectaDelimitadorVirgula(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("pe_clientes.csv");
        Files.writeString(csv, """
                cnpj_completo,razao_social,endereco_completo,email,ano_abertura,telefones
                11222333000181,Gamma Ltda,Rua D,10,d@g.com,2011,81-34444555
                """, StandardCharsets.ISO_8859_1);

        MotorClassificacaoService.Resultado r =
                new MotorClassificacaoService(List.of("PE")).classificarArquivo(csv);

        assertThat(r.uf()).isEqualTo("PE");
        assertThat(r.registrosUnicos()).isEqualTo(1);
        assertThat(r.contagemPorOperadora()).containsEntry("FIXO", 1L);
    }

    @Test
    void rejeitaNomeSemUfPermitida(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("sp_ativos.csv");
        Files.write(csv, CSV_AL);

        MotorClassificacaoService motor = new MotorClassificacaoService(List.of("AL", "PE"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> motor.classificarArquivo(csv));
    }

    @Test
    void extraiUfDeNomeComSufixo(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("ativos_ma.csv");
        Files.write(csv, CSV_AL);

        MotorClassificacaoService.Resultado r =
                new MotorClassificacaoService(List.of("MA")).classificarArquivo(csv);
        assertThat(r.uf()).isEqualTo("MA");
    }

    @Test
    void linhaIncompletaEDescartadaSemErro(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("ba_ativos.csv");
        Files.writeString(csv, """
                cnpj_completo;razao_social;endereco_completo;email;ano_abertura;telefones
                11222333000181;Sem Telefones;Rua E;;2012
                33444555000166;Delta SA;Rua F;f@g.com;2013;71-35555666
                """, StandardCharsets.ISO_8859_1);

        MotorClassificacaoService.Resultado r =
                new MotorClassificacaoService(List.of("BA")).classificarArquivo(csv);

        assertThat(r.linhasLidas()).isEqualTo(2);
        assertThat(r.registrosUnicos()).isEqualTo(1); // linha incompleta ignorada (paridade legado)
    }
}
