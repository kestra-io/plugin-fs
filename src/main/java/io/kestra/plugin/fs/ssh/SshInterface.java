package io.kestra.plugin.fs.ssh;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface SshInterface {
    String ALLOW_OPEN_SSH_CONFIG = "allow-open-ssh-config";

    @Schema(
        title = "Hostname of the remote server"
    )
    @NotNull
    Property<String> getHost();

    @Schema(
        title = "Port of the remote server"
    )
    Property<String> getPort();

    @Schema(
        title = "Authentication method"
    )
    @NotNull
    Property<AuthMethod> getAuthMethod();

    @Schema(
        title = "Username on the remote server, required for password auth method"
    )
    Property<String> getUsername();

    @Schema(
        title = "Password on the remote server, required for password auth method"
    )
    Property<String> getPassword();

    @Schema(
        title = "Private SSH Key to authenticate, required for pubkey auth method"
    )
    Property<String> getPrivateKey();

    @Schema(
        title = "Passphrase used in order to unseal the private key, optional for pubkey auth method"
    )
    Property<String> getPrivateKeyPassphrase();

    @Schema(
        title = "OpenSSH configuration directory in case the authentication method is `OPEN_SSH`.",
        description = "Deprecated. Use openSSHConfigPath instead."
    )
    @Deprecated
    Property<String> getOpenSSHConfigDir();

    @Schema(
        title = "OpenSSH configuration file path in case the authentication method is `OPEN_SSH`."
    )
    Property<String> getOpenSSHConfigPath();

    enum AuthMethod {
        PASSWORD,
        PUBLIC_KEY,
        OPEN_SSH
    }
}
