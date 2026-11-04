package br.com.consultaoperadora.backend.entity;

/**
 * Ciclo de vida de um job de processamento (Fase 4).
 * NOVO -> EM_EXECUCAO -> CONCLUIDO | FALHA
 */
public enum StatusProcessamento {
    NOVO,
    EM_EXECUCAO,
    CONCLUIDO,
    FALHA
}
