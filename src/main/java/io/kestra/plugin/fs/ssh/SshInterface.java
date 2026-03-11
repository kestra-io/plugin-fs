package io.kestra.plugin.fs.ssh;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface SshInterface {
    String ALLOW_OPEN_SSH_CONFIG = "allow-open-ssh-config";

    @Schema(
        title = "Remote host"
    )
    @NotNull
    Property<String> getHost();

    @Schema(
        title = "Remote port"
    )
    Property<String> getPort();

    @Schema(
        title = "Authentication method"
    )
    @NotNull
    Property<AuthMethod> getAuthMethod();

    @Schema(
        title = "Username",
        description = "Required for PASSWORD and PUBLIC_KEY methods."
    )
    Property<String> getUsername();

    @Schema(
        title = "Password",
        description = "Required for PASSWORD auth; optional for OPEN_SSH when config supplies credentials."
    )
    Property<String> getPassword();

    @Schema(
        title = "Private SSH key",
        description = "OpenSSH private key content for PUBLIC_KEY auth."
    )
    Property<String> getPrivateKey();

    @Schema(
        title = "Private key passphrase",
        description = "Optional passphrase for the private key."
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

    @Schema(
        title = "Proxy command",
        description = "Optional local command used to establish the SSH transport (OpenSSH `ProxyCommand` semantics)."
    )
    Property<String> getProxyCommand();

    enum AuthMethod {
        PASSWORD,
        PUBLIC_KEY,
        OPEN_SSH
    }
}
