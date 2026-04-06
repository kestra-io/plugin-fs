package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import io.kestra.core.models.annotations.PluginProperty;

public interface SftpInterface {
    @Schema(
        title = "SSH private key (PEM)",
        description = "PEM-formatted private key for public key auth. Convert OpenSSH keys with `ssh-keygen -m PEM`."
    )
    @PluginProperty(group = "connection")
    Property<String> getKeyfile();

    @Schema(
        title = "Passphrase for the SSH key"
    )
    @PluginProperty(group = "advanced")
    Property<String> getPassphrase();

    @Deprecated
    @Schema(
        title = "Deprecated proxy host",
        description = "Use `proxyAddress` instead; retained for backward compatibility."
    )
    @PluginProperty(group = "connection")
    Property<String> getProxyHost();

    @Schema(
        title = "SFTP proxy address"
    )
    @PluginProperty(group = "advanced")
    Property<String> getProxyAddress();

    @Schema(
        title = "SFTP proxy port"
    )
    @PluginProperty(group = "connection")
    Property<String> getProxyPort();

    @Deprecated
    @Schema(
        title = "Deprecated proxy user",
        description = "Use `proxyUsername` instead; retained for backward compatibility."
    )
    @PluginProperty(group = "advanced")
    Property<String> getProxyUser();

    @Schema(
        title = "SFTP proxy username"
    )
    @PluginProperty(group = "connection")
    Property<String> getProxyUsername();

    @Schema(
        title = "SFTP proxy password"
    )
    @PluginProperty(group = "connection")
    Property<String> getProxyPassword();

    @Schema(
        title = "SFTP proxy type",
        description = "One of SOCKS5, STREAM, or HTTP."
    )
    @PluginProperty(group = "advanced")
    Property<String> getProxyType();

    @Schema(
        title = "Treat path as user home root",
        description = "If true (default), remote paths are resolved relative to the authenticated user's home directory."
    )
    @PluginProperty(group = "advanced")
    Property<Boolean> getRootDir();

    @Schema(
        title = "Key exchange algorithm",
        description = "Override the KEX algorithm (e.g. diffie-hellman-group14-sha1, diffie-hellman-group-exchange-sha256)."
    )
    @PluginProperty(group = "advanced")
    Property<String> getKeyExchangeAlgorithm();
}
