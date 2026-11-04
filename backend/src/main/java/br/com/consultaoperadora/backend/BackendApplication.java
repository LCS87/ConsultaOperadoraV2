package br.com.consultaoperadora.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Bootstrap da API REST (Fase 3).
 * O nucleo de regras permanece no modulo {@code consulta-operadora-core};
 * aqui apenas expomos as capacidades via HTTP + OpenAPI.
 *
 * Fase 4: a entidade JPA {@code EmpresaRegistro} vive no core (pacote
 * {@code br.com.consultaoperadora.model}), fora da hierarquia de pacotes deste
 * bootstrap — por isso o scan explícito de entidades abaixo.
 */
@SpringBootApplication
@org.springframework.boot.autoconfigure.domain.EntityScan(
        basePackages = {"br.com.consultaoperadora.backend.entity", "br.com.consultaoperadora.model"})
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
