package br.com.consultaoperadora.backend.repository;

import br.com.consultaoperadora.backend.entity.DuplicidadeLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;

/** Auditoria persistente de descartes por duplicidade (Fase 4). */
public interface DuplicidadeLogRepository extends JpaRepository<DuplicidadeLogEntity, Long> {

    long countByJobId(Long jobId);
}
