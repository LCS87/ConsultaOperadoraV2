package br.com.consultaoperadora.backend.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.util.Map;

/**
 * Tratamento uniforme de erros da API (Fase 3): mensagens claras em JSON,
 * sem stack trace para o cliente.
 */
@RestControllerAdvice
public class ApiErrorHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, Object> badRequest(IllegalArgumentException e) {
        return Map.of("status", 400, "erro", "Requisicao invalida", "mensagem", e.getMessage());
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    @ResponseStatus(HttpStatus.PAYLOAD_TOO_LARGE)
    public Map<String, Object> tooLarge(MaxUploadSizeExceededException e) {
        return Map.of("status", 413, "erro", "Arquivo excede o limite de upload",
                "mensagem", "Reduza o tamanho do CSV ou aumente spring.servlet.multipart.max-file-size.");
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public Map<String, Object> unprocessable(IllegalStateException e) {
        return Map.of("status", 422, "erro", "Falha ao processar o arquivo", "mensagem", e.getMessage());
    }
}
