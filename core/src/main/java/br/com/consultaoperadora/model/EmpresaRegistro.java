package br.com.consultaoperadora.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.Arrays;

/**
 * Registro de empresa classificado por operadora (Fase 1 → promovido a entidade
 * na Fase 4 — Persistência).
 *
 * As colunas seguem a ordem do arquivo de exportação:
 * cnpj_completo, razao_social, endereco_completo, email, ano_abertura, telefones.
 *
 * O campo {@code columns} mantém o contrato exato com OpenCSV/CLI (6 posições);
 * os getters derivados continuam funcionando para quem usa a classe como DTO.
 * A UNIQUE em {@code cnpj_digitos} implementa a deduplicação PERSISTENTE entre
 * execuções (critério de aceite da Fase 4) — reprocessar o mesmo arquivo não
 * duplica CNPJs.
 */
@Entity
@Table(name = "empresa_registro",
        uniqueConstraints = @UniqueConstraint(name = "uk_empresa_cnpj", columnNames = "cnpj_digitos"),
        indexes = {
                @Index(name = "idx_empresa_operadora_uf", columnList = "operadora,uf"),
                @Index(name = "idx_empresa_job", columnList = "job_id")
        })
public class EmpresaRegistro {

    public static final int EXPECTED_MIN_COLUMNS = 6;
    public static final String[] EXPORT_HEADER = {
            "cnpj_completo", "razao_social", "endereco_completo", "email", "ano_abertura", "telefones"};

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 14 dígitos normalizados (sem máscara) — chave de deduplicação persistente. */
    @Column(name = "cnpj_digitos", length = 14)
    private String cnpjDigitos;

    @Column(length = 20)
    private String operadora;   // nome do enum (CLARO|VIVO|TIM|OI|FIXO|SEM_OPERADORA)

    @Column(length = 2)
    private String uf;

    @Column(name = "telefone_normalizado", length = 32)
    private String telefoneNormalizado;

    @Column(name = "job_id")
    private Long jobId;

    /** Colunas cruas do CSV (contrato legado com OpenCSV; serializadas para TEXT). */
    @Column(columnDefinition = "TEXT")
    private String[] columns;

    public EmpresaRegistro() {
        // construtor JPA
    }

    public EmpresaRegistro(String[] columns) {
        this.columns = columns;
    }

    /** Fábrica usada pelo pipeline CLI (mantém assinatura original da Fase 1). */
    public static EmpresaRegistro fromRow(String[] rowData) {
        if (rowData == null || rowData.length < EXPECTED_MIN_COLUMNS) {
            return null;
        }
        return new EmpresaRegistro(Arrays.copyOfRange(rowData, 0, EXPECTED_MIN_COLUMNS));
    }

    /** Fábrica da ingestão REST (Fase 4): DTO + resultado da classificação. */
    public static EmpresaRegistro of(String[] columns, String cnpjDigitos, Operadora operadora,
                                     String uf, String telefoneNormalizado, Long jobId) {
        EmpresaRegistro e = new EmpresaRegistro(columns);
        e.cnpjDigitos = cnpjDigitos;
        e.operadora = operadora.name();
        e.uf = uf;
        e.telefoneNormalizado = telefoneNormalizado;
        e.jobId = jobId;
        return e;
    }

    public String cnpj() {
        return columns[0].trim();
    }

    public String razaoSocial() {
        return columns[1];
    }

    public String enderecoCompleto() {
        return columns[2];
    }

    public String email() {
        return columns[3];
    }

    public String anoAbertura() {
        return columns[4];
    }

    /** Coluna bruta de telefones (entrada do TelefoneNormalizer/OperadoraClassifier). */
    public String telefonesRaw() {
        return columns[5];
    }

    /** Cópia defensiva das colunas — usada pelo CsvWriterService. */
    public String[] asColumns() {
        return columns.clone();
    }

    // ----- getters/setters JPA (campos derivados, Fase 4) -----

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getCnpjDigitos() { return cnpjDigitos; }
    public void setCnpjDigitos(String cnpjDigitos) { this.cnpjDigitos = cnpjDigitos; }

    public String getOperadora() { return operadora; }
    public void setOperadora(String operadora) { this.operadora = operadora; }

    public String getUf() { return uf; }
    public void setUf(String uf) { this.uf = uf; }

    public String getTelefoneNormalizado() { return telefoneNormalizado; }
    public void setTelefoneNormalizado(String telefoneNormalizado) { this.telefoneNormalizado = telefoneNormalizado; }

    public Long getJobId() { return jobId; }
    public void setJobId(Long jobId) { this.jobId = jobId; }

    public String[] getColumns() { return columns; }
    public void setColumns(String[] columns) { this.columns = columns; }
}
