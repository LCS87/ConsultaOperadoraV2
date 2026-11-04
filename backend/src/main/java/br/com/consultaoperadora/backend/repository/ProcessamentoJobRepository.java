package br.com.consultaoperadora.backend.repository;

import br.com.consultaoperadora.backend.entity.ProcessamentoJobEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/** Repositório de jobs de processamento (Fase 4). */
public interface ProcessamentoJobRepository extends JpaRepository<ProcessamentoJobEntity, Long> {

    List<ProcessamentoJobEntity> findTop20ByOrderByIdDesc();
}
