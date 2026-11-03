package br.com.consultaoperadora.backend.service;

import br.com.consultaoperadora.batch.MotorClassificacaoService;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

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
        String original = sanitizeFileName(file.getOriginalFilename());
        if (!original.toLowerCase().endsWith(".csv")) {
            throw new IllegalArgumentException("Apenas arquivos .csv sao aceitos (recebido: " + original + ").");
        }
        // Nome normalizado para o padrao esperado pelo motor: <uf>_*.csv
        String nomeParaMotor = normalizarNomeParaMotor(original);
        Path destino = Files.createTempFile(uploadDir, "up-", "-" + nomeParaMotor);
        try (InputStream in = file.getInputStream()) {
            Files.copy(in, destino, StandardCopyOption.REPLACE_EXISTING);
        }
        try {
            return motor.classificarArquivo(destino);
        } finally {
            Files.deleteIfExists(destino);
        }
    }

    /** Remove caminho/traversal e caracteres problematicos; mantem apenas o basename. */
    static String sanitizeFileName(String originalFilename) {
        if (originalFilename == null || originalFilename.isBlank()) {
            return "upload.csv";
        }
        String base = Path.of(originalFilename.replace('\\', '/')).getFileName().toString();
        base = base.replaceAll("[^A-Za-z0-9._-]", "_");
        return base.isBlank() ? "upload.csv" : base;
    }

    /**
     * Garante que o nome final cas com os padroes do motor ({@code uf_*.csv} ou
     * {@code *_uf.csv}). Se o usuario enviar ex.: "AL-2024.csv", convertemos para
     * "al_2024.csv" (hifen apos a UF vira underscore).
     */
    static String normalizarNomeParaMotor(String nomeSanitizado) {
        String lower = nomeSanitizado.toLowerCase(Locale.ROOT);
        // Substitui o primeiro hifen por underscore (UF-prefixo): "al-x.csv" -> "al_x.csv"
        return lower.replaceFirst("^([a-z]{2})-", "$1_");
    }

    public MotorClassificacaoService.Resultado classificarArquivoLocal(Path csvPath) throws IOException {
        return motor.classificarArquivo(csvPath);
    }
}
