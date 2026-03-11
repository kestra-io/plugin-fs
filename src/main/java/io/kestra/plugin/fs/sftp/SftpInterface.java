package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface SftpInterface {
    @Schema(
        title = "SSH private key (PEM)",
        description = "PEM-formatted private key for public key auth. Convert OpenSSH keys with `ssh-keygen -m PEM`."
    )
    Property<String> getKeyfile();

    @Schema(
        title = "Passphrase for the SSH key"
    )
    Property<String> getPassphrase();

    @Deprecated
    @Schema(
        title = "Deprecated proxy host",
        description = "Use `proxyAddress` instead; retained for backward compatibility."
    )
    Property<String> getProxyHost();

    @Schema(
        title = "SFTP proxy address"
    )
    Property<String> getProxyAddress();

    @Schema(
        title = "SFTP proxy port"
    )
    Property<String> getProxyPort();

    @Deprecated
    @Schema(
        title = "Deprecated proxy user",
        description = "Use `proxyUsername` instead; retained for backward compatibility."
    )
    Property<String> getProxyUser();

    @Schema(
        title = "SFTP proxy username"
    )
    Property<String> getProxyUsername();

    @Schema(
        title = "SFTP proxy password"
    )
    Property<String> getProxyPassword();

    @Schema(
        title = "SFTP proxy type",
        description = "One of SOCKS5, STREAM, or HTTP."
    )
    Property<String> getProxyType();

    @Schema(
        title = "Treat path as user home root",
        description = "If true (default), remote paths are resolved relative to the authenticated user's home directory."
    )
    Property<Boolean> getRootDir();

    @Schema(
        title = "Key exchange algorithm",
        description = "Override the KEX algorithm (e.g. diffie-hellman-group14-sha1, diffie-hellman-group-exchange-sha256)."
    )
    Property<String> getKeyExchangeAlgorithm();
}
