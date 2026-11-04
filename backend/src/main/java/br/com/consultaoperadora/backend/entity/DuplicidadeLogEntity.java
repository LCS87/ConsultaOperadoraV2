package br.com.consultaoperadora.backend.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;

/**
 * Auditoria de descartes por duplicidade (Fase 4): registra QUAL CNPJ foi
 * descartado, em QUAL job e POR QUE (já existia no banco ou repetido dentro
 * do próprio arquivo). Substitui o HashSet volátil do CLI por evidência
 * persistente e consultável.
 */
@Entity
@Table(name = "duplicidade_log",
        uniqueConstraints = @UniqueConstraint(name = "uk_dup_job_cnpj", columnNames = {"job_id", "cnpj"}),
        indexes = @Index(name = "idx_dup_cnpj", columnList = "cnpj"))
public class DuplicidadeLogEntity {

    public static final String MOTIVO_JA_EXISTENTE_BANCO = "CNPJ_JA_PERSISTIDO";
    public static final String MOTIVO_REPETIDO_NO_ARQUIVO = "DUPLICADO_INTERNO";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(nullable = false, length = 14)
    private String cnpj;

    @Column(name = "arquivo_origem", length = 255)
    private String arquivoOrigem;

    @Column(nullable = false, length = 40)
    private String motivo;

    @Column(name = "ocorrido_em", nullable = false)
    private Instant ocorridoEm;

    public static DuplicidadeLogEntity de(Long jobId, String cnpj, String arquivo, String motivo) {
        DuplicidadeLogEntity l = new DuplicidadeLogEntity();
        l.jobId = jobId;
        l.cnpj = cnpj;
        l.arquivoOrigem = arquivo;
        l.motivo = motivo;
        return l;
    }

    @PrePersist
    void onCreate() {
        if (ocorridoEm == null) {
            ocorridoEm = Instant.now();
        }
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getJobId() { return jobId; }
    public void setJobId(Long jobId) { this.jobId = jobId; }

    public String getCnpj() { return cnpj; }
    public void setCnpj(String cnpj) { this.cnpj = cnpj; }

    public String getArquivoOrigem() { return arquivoOrigem; }
    public void setArquivoOrigem(String arquivoOrigem) { this.arquivoOrigem = arquivoOrigem; }

    public String getMotivo() { return motivo; }
    public void setMotivo(String motivo) { this.motivo = motivo; }

    public Instant getOcorridoEm() { return ocorridoEm; }
    public void setOcorridoEm(Instant ocorridoEm) { this.ocorridoEm = ocorridoEm; }
}
