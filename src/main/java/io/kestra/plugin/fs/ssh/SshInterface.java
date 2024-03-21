package io.kestra.plugin.fs.ssh;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface SshInterface {
    @Schema(
        title = "Hostname of the remote server"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getHost();

    @Schema(
        title = "Port of the remote server"
    )
    @PluginProperty(dynamic = true)
    String getPort();

    @Schema(
        title = "Authentication method"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    AuthMethod getAuthMethod();

    @Schema(
        title = "Username on the remote server, required for password auth method"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getUsername();

    @Schema(
        title = "Password on the remote server, required for password auth method"
    )
    @PluginProperty(dynamic = true)
    String getPassword();

    @Schema(
        title = "Private SSH Key to authenticate, required for pubkey auth method"
    )
    @PluginProperty(dynamic = true)
    String getPrivateKey();

    @Schema(
        title = "Passphrase used in order to unseal the private key, optional for pubkey auth method"
    )
    @PluginProperty(dynamic = true)
    String getPrivateKeyPassphrase();

    enum AuthMethod {
        PASSWORD,
        PUBLIC_KEY
    }
}
