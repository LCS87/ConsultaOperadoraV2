package br.com.consultaoperadora.backend.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Metadados de um CSV recebido via upload (Fase 4 — Persistência).
 *
 * Guarda inclusive o que o motor "descobre" em runtime (encoding assumido e
 * delimitador detectado), permitindo auditoria de como cada arquivo foi lido.
 */
@Entity
@Table(name = "arquivo_origem")
public class ArquivoOrigemEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "nome_arquivo", nullable = false, length = 255)
    private String nomeArquivo;

    @Column(length = 2)
    private String uf;

    @Column(nullable = false, length = 32)
    private String encoding;

    @Column(name = "delimitador_detectado", length = 8)
    private String delimitadorDetectado;

    @Column(name = "tamanho_bytes")
    private Long tamanhoBytes;

    @Column(name = "hash_sha256", length = 64)
    private String hashSha256;

    @Column(name = "criado_em", nullable = false)
    private Instant criadoEm;

    @PrePersist
    void onCreate() {
        if (criadoEm == null) {
            criadoEm = Instant.now();
        }
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getNomeArquivo() { return nomeArquivo; }
    public void setNomeArquivo(String nomeArquivo) { this.nomeArquivo = nomeArquivo; }

    public String getUf() { return uf; }
    public void setUf(String uf) { this.uf = uf; }

    public String getEncoding() { return encoding; }
    public void setEncoding(String encoding) { this.encoding = encoding; }

    public String getDelimitadorDetectado() { return delimitadorDetectado; }
    public void setDelimitadorDetectado(String delimitadorDetectado) { this.delimitadorDetectado = delimitadorDetectado; }

    public Long getTamanhoBytes() { return tamanhoBytes; }
    public void setTamanhoBytes(Long tamanhoBytes) { this.tamanhoBytes = tamanhoBytes; }

    public String getHashSha256() { return hashSha256; }
    public void setHashSha256(String hashSha256) { this.hashSha256 = hashSha256; }

    public Instant getCriadoEm() { return criadoEm; }
    public void setCriadoEm(Instant criadoEm) { this.criadoEm = criadoEm; }
}
