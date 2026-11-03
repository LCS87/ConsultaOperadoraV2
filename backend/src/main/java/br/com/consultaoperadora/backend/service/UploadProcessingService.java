package br.com.consultaoperadora.backend.service;

import br.com.consultaoperadora.batch.MotorClassificacaoService;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Camada de aplicacao da API: recebe uploads, materializa em disco temporario
 * (o motor do core le a partir de Path, com deteccao de delimitador por streaming)
 * e delega ao MotorClassificacaoService — regras 100% no core, zero duplicacao.
 */
@Service
public class UploadProcessingService {

    private final MotorClassificacaoService motor;
    private final Path uploadDir;

    public UploadProcessingService(MotorClassificacaoService motor) throws IOException {
        this.motor = motor;
        this.uploadDir = Files.createTempDirectory("consulta-operadora-uploads");
        this.uploadDir.toFile().deleteOnExit();
    }

    public MotorClassificacaoService.Resultado processarUpload(MultipartFile file) throws IOException {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("Arquivo vazio ou ausente no multipart 'file'.");
        }
        String original = Path.of(file.getOriginalFilename() == null ? "upload.csv"
                : file.getOriginalFilename()).getFileName().toString(); // sanitiza path traversal
        if (!original.toLowerCase().endsWith(".csv")) {
            throw new IllegalArgumentException("Apenas arquivos .csv sao aceitos (recebido: " + original + ").");
        }
        Path destino = Files.createTempFile(uploadDir, "up-", ".csv");
        try (InputStream in = file.getInputStream()) {
            Files.copy(in, destino, StandardCopyOption.REPLACE_EXISTING);
        }
        try {
            return motor.classificarArquivo(destino.resolveSibling(original));
        } finally {
            Files.deleteIfExists(destino);
        }
    }

    public MotorClassificacaoService.Resultado classificarArquivoLocal(Path csvPath) throws IOException {
        return motor.classificarArquivo(csvPath);
    }
}
