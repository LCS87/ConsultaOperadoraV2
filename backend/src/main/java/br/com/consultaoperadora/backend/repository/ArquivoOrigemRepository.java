package br.com.consultaoperadora.backend.repository;

import br.com.consultaoperadora.backend.entity.ArquivoOrigemEntity;
import org.springframework.data.jpa.repository.JpaRepository;

/** Repositório de arquivos recebidos (Fase 4). */
public interface ArquivoOrigemRepository extends JpaRepository<ArquivoOrigemEntity, Long> {
}
