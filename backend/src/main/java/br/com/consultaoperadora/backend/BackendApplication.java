package br.com.consultaoperadora.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Bootstrap da API REST (Fase 3).
 * O nucleo de regras permanece no modulo {@code consulta-operadora-core};
 * aqui apenas expomos as capacidades via HTTP + OpenAPI.
 */
@SpringBootApplication
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}
