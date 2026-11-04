package br.com.consultaoperadora.backend.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.Duration;
import java.time.Instant;

/**
 * Job de processamento (Fase 4): acompanha o ciclo de vida da ingestão de um
 * arquivo e acumula métricas (linhas lidas, inseridos, duplicados descartados).
 */
@Entity
@Table(name = "processamento_job")
public class ProcessamentoJobEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "arquivo_id", nullable = false)
    private ArquivoOrigemEntity arquivo;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private StatusProcessamento status = StatusProcessamento.NOVO;

    @Column(name = "linhas_lidas", nullable = false)
    private long linhasLidas;

    @Column(name = "registros_inseridos", nullable = false)
    private int registrosInseridos;

    @Column(name = "duplicados_descartados", nullable = false)
    private int duplicadosDescartados;

    @Column(name = "cnpj_invalidos_descartados", nullable = false)
    private int cnpjInvalidosDescartados;

    @Column(name = "por_operadora", length = 1000)
    private String porOperadora; // resumo legível: "CLARO=12; FIXO=3"

    @Column(name = "iniciado_em")
    private Instant iniciadoEm;

    @Column(name = "finalizado_em")
    private Instant finalizadoEm;

    @Column(name = "mensagem_erro", length = 500)
    private String mensagemErro;

    @PrePersist
    void onCreate() {
        if (status == null) {
            status = StatusProcessamento.NOVO;
        }
    }

    /** Tempo total do job, ou null se ainda não terminou. */
    public Duration duracao() {
        if (iniciadoEm == null || finalizadoEm == null) {
            return null;
        }
        return Duration.between(iniciadoEm, finalizadoEm);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public ArquivoOrigemEntity getArquivo() { return arquivo; }
    public void setArquivo(ArquivoOrigemEntity arquivo) { this.arquivo = arquivo; }

    public StatusProcessamento getStatus() { return status; }
    public void setStatus(StatusProcessamento status) { this.status = status; }

    public long getLinhasLidas() { return linhasLidas; }
    public void setLinhasLidas(long linhasLidas) { this.linhasLidas = linhasLidas; }

    public int getRegistrosInseridos() { return registrosInseridos; }
    public void setRegistrosInseridos(int registrosInseridos) { this.registrosInseridos = registrosInseridos; }

    public int getDuplicadosDescartados() { return duplicadosDescartados; }
    public void setDuplicadosDescartados(int duplicadosDescartados) { this.duplicadosDescartados = duplicadosDescartados; }

    public int getCnpjInvalidosDescartados() { return cnpjInvalidosDescartados; }
    public void setCnpjInvalidosDescartados(int cnpjInvalidosDescartados) { this.cnpjInvalidosDescartados = cnpjInvalidosDescartados; }

    public String getPorOperadora() { return porOperadora; }
    public void setPorOperadora(String porOperadora) { this.porOperadora = porOperadora; }

    public Instant getIniciadoEm() { return iniciadoEm; }
    public void setIniciadoEm(Instant iniciadoEm) { this.iniciadoEm = iniciadoEm; }

    public Instant getFinalizadoEm() { return finalizadoEm; }
    public void setFinalizadoEm(Instant finalizadoEm) { this.finalizadoEm = finalizadoEm; }

    public String getMensagemErro() { return mensagemErro; }
    public void setMensagemErro(String mensagemErro) { this.mensagemErro = mensagemErro; }
}
