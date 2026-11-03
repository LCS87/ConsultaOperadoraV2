package br.com.consultaoperadora.service;

import br.com.consultaoperadora.model.EmpresaRegistro;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fase 2 — cobertura completa de {@link DeduplicationService} e
 * {@link EmpresaRegistro} (getters individuais, reset e cópia defensiva).
 */
class DeduplicacaoERegistroTest {

    // ---------- DeduplicationService ----------

    @Test
    void resetLimpaHistoricoEPermiteReuso() {
        DeduplicationService dedup = new DeduplicationService();
        assertThat(dedup.registerIfNew("11222333000181")).isTrue();
        assertThat(dedup.registerIfNew("99888777000116")).isTrue();
        assertThat(dedup.uniqueCount()).isEqualTo(2);

        dedup.reset();

        assertThat(dedup.uniqueCount()).isZero();
        assertThat(dedup.registerIfNew("11222333000181")).isTrue(); // visto de novo após reset
    }

    // ---------- EmpresaRegistro ----------

    private static EmpresaRegistro registro() {
        return EmpresaRegistro.fromRow(new String[]{
                " 11222333000181 ", "ACME LTDA", "Rua A, 100", "contato@acme.com", "2020", "(82) 3311-1200"});
    }

    @Test
    void gettersExpoemColunasNaOrdemDoExport() {
        EmpresaRegistro r = registro();
        assertThat(r.cnpj()).isEqualTo("11222333000181");      // trim aplicado
        assertThat(r.razaoSocial()).isEqualTo("ACME LTDA");
        assertThat(r.enderecoCompleto()).isEqualTo("Rua A, 100");
        assertThat(r.email()).isEqualTo("contato@acme.com");
        assertThat(r.anoAbertura()).isEqualTo("2020");
        assertThat(r.telefonesRaw()).isEqualTo("(82) 3311-1200");
    }

    @Test
    void asColumnsDevolveCopiaDefensiva() {
        EmpresaRegistro r = registro();
        String[] c1 = r.asColumns();
        String[] c2 = r.asColumns();
        assertThat(c1).isNotSameAs(c2);           // arrays distintos
        assertThat(c1).containsExactlyElementsOf(java.util.List.of(
                " 11222333000181 ", "ACME LTDA", "Rua A, 100", "contato@acme.com", "2020", "(82) 3311-1200"));
        c1[1] = "MUTADA";
        assertThat(r.razaoSocial()).isEqualTo("ACME LTDA"); // estado interno intacto
    }

    @Test
    void headerDeExportTemSeisColunasNaOrdemLegada() {
        assertThat(EmpresaRegistro.EXPORT_HEADER).containsExactly(
                "cnpj_completo", "razao_social", "endereco_completo", "email", "ano_abertura", "telefones");
        assertThat(EmpresaRegistro.EXPECTED_MIN_COLUMNS).isEqualTo(6);
    }
}
