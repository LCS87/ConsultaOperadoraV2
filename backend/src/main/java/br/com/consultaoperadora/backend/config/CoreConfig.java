package br.com.consultaoperadora.backend.config;

import br.com.consultaoperadora.batch.MotorClassificacaoService;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Wiring Spring do motor de classificacao (POJO do core) e metadados OpenAPI.
 */
@Configuration
public class CoreConfig {

    @Bean
    public MotorClassificacaoService motorClassificacaoService(
            @Value("${app.ufs-permitidas:AL,BA,CE,MA,PB,PE,PI,RN,SE}") String ufsCsv) {
        List<String> ufs = List.of(ufsCsv.split("\\s*,\\s*"));
        return new MotorClassificacaoService(ufs);
    }

    @Bean
    public OpenAPI consultaOperadoraOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Consulta Operadora API")
                .version("1.0.0")
                .description("API REST de classificacao de operadoras por telefone "
                        + "(Nordeste) — Fase 3 do plano de melhoria."));
    }
}
