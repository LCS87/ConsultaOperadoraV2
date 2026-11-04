package br.com.consultaoperadora.backend.repository;

import br.com.consultaoperadora.backend.entity.EmpresaRegistroEntity;
import br.com.consultaoperadora.model.Operadora;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

/**
 * Repositório de registros classificados (Fase 4).
 * Busca paginada com filtros opcionais de operadora/UF — mesma semântica dos
 * arquivos "{UF} - {OPERADORA}.csv" gerados pelo CLI, agora consultável.
 */
public interface EmpresaRegistroRepository extends JpaRepository<EmpresaRegistroEntity, Long> {

    /** Pessimistic lock: serializa ingestões concorrentes e elimina corrida no UNIQUE de cnpj. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select count(e) > 0 from EmpresaRegistroEntity e where e.cnpj = :cnpj")
    boolean existsByCnpjForUpdate(@Param("cnpj") String cnpj);

    boolean existsByCnpj(String cnpj);

    @Query("""
            select e from EmpresaRegistroEntity e
            where (:operadora is null or e.operadora = :operadora)
              and (:uf is null or e.uf = :uf)
            """)
    Page<EmpresaRegistroEntity> buscar(@Param("operadora") Operadora operadora,
                                       @Param("uf") String uf,
                                       Pageable pageable);

    @Query("""
            select e.operadora, count(e) from EmpresaRegistroEntity e
            group by e.operadora order by count(e) desc
            """)
    java.util.List<Object[]> contagemPorOperadora();

    @Query("""
            select e.operadora, e.uf, count(e) from EmpresaRegistroEntity e
            group by e.operadora, e.uf
            """)
    java.util.List<Object[]> contagemPorOperadoraUf();
}
