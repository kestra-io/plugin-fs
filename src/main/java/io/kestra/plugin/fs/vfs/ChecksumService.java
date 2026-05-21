package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.KestraRuntimeException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class ChecksumService {

    private static final int BUFFER_SIZE = 8 * 1024;

    private ChecksumService() {
    }

    public enum Algorithm {
        MD5("MD5"),
        SHA_1("SHA-1"),
        SHA_256("SHA-256"),
        SHA_512("SHA-512");

        private final String jcaName;

        Algorithm(String jcaName) {
            this.jcaName = jcaName;
        }

        public String jcaName() {
            return jcaName;
        }
    }

    public static String compute(Path file, Algorithm algorithm) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm.jcaName());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unsupported checksum algorithm: " + algorithm.jcaName(), e);
        }

        try (InputStream in = new BufferedInputStream(Files.newInputStream(file), BUFFER_SIZE);
             DigestInputStream digestIn = new DigestInputStream(in, digest)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            while (digestIn.read(buffer) != -1) {
                // streaming — DigestInputStream updates the digest
            }
        }

        return HexFormat.of().formatHex(digest.digest());
    }

    public static String verify(Path file, Algorithm algorithm, String expected) throws IOException {
        if (expected == null || expected.isBlank()) {
            throw new KestraRuntimeException(
                "Checksum validation is enabled but no `checksumExpected` value was provided."
            );
        }

        String computed = compute(file, algorithm);
        if (!computed.equalsIgnoreCase(expected.trim())) {
            throw new KestraRuntimeException(String.format(
                "Checksum mismatch for file '%s' — expected '%s' but computed '%s' using %s.",
                file, expected, computed, algorithm.jcaName()
            ));
        }

        return computed;
    }
}
