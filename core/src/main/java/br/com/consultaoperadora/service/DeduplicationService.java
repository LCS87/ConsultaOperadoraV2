package br.com.consultaoperadora.service;

import java.util.HashSet;
import java.util.Set;

/**
 * Deduplicação de CNPJs por arquivo (Fase 1) — encapsula o {@code HashSet} local
 * que existia dentro de {@code Main.processFile}.
 *
 * Escopo preservado: um deduplicador é criado por arquivo (o legado não cruzava
 * duplicados entre arquivos diferentes). Instância NÃO thread-safe — uma por thread.
 */
public final class DeduplicationService {

    private final Set<String> vistos = new HashSet<>();

    /** @return true se o CNPJ ainda não foi visto; registra automaticamente. */
    public boolean registerIfNew(String cnpj) {
        return vistos.add(cnpj);
    }

    public int uniqueCount() {
        return vistos.size();
    }

    public void reset() {
        vistos.clear();
    }
}
