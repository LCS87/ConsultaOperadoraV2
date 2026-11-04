package br.com.consultaoperadora.backend.repository;

import br.com.consultaoperadora.model.EmpresaRegistro;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Repositório de registros classificados (Fase 4).
 * Busca paginada com filtros opcionais de operadora/UF — mesma semântica dos
 * arquivos "{UF} - {OPERADORA}.csv" gerados pelo CLI, agora consultável.
 *
 * Observação: {@code operadora} é mapeada como String (nome do enum) na
 * entidade do core, então os parâmetros de filtro são Strings já normalizadas.
 */
public interface EmpresaRegistroRepository extends JpaRepository<EmpresaRegistro, Long> {

    /** Pessimistic lock: serializa ingestões concorrentes e elimina corrida no UNIQUE de cnpj. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select count(e) > 0 from EmpresaRegistro e where e.cnpjDigitos = :cnpj")
    boolean existsByCnpjForUpdate(@Param("cnpj") String cnpj);

    boolean existsByCnpjDigitos(String cnpj);

    @Query("""
            select e from EmpresaRegistro e
            where (:operadora is null or e.operadora = :operadora)
              and (:uf is null or e.uf = :uf)
            """)
    Page<EmpresaRegistro> buscar(@Param("operadora") String operadora,
                                 @Param("uf") String uf,
                                 Pageable pageable);

    @Query("""
            select e.operadora, count(e) from EmpresaRegistro e
            group by e.operadora order by count(e) desc
            """)
    java.util.List<Object[]> contagemPorOperadora();

    @Query("""
            select e.operadora, e.uf, count(e) from EmpresaRegistro e
            group by e.operadora, e.uf
            """)
    java.util.List<Object[]> contagemPorOperadoraUf();
}
