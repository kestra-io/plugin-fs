package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface SftpInterface {
    @Schema(
        title = "Private keyfile in the PEM file format to connect to a remote server using SSH",
        description = "To generate a PEM format key from OpenSSH, use the following command: `ssh-keygen -m PEM`"
    )
    Property<String> getKeyfile();

    @Schema(
        title = "Passphrase of the ssh key"
    )
    Property<String> getPassphrase();

    @Schema(
        title = "SFTP proxy host"
    )
    Property<String> getProxyHost();

    @Schema(
        title = "SFTP proxy port"
    )
    Property<String> getProxyPort();

    @Schema(
        title = "SFTP proxy user"
    )
    Property<String> getProxyUser();

    @Schema(
        title = "SFTP proxy password"
    )
    Property<String> getProxyPassword();

    @Schema(
        title = "SFTP proxy type"
    )
    Property<String> getProxyType();

    @Schema(
        title = "Is the path relative to the users home directory"
    )
    Property<Boolean> getRootDir();

    @Schema(
        title = "Configures Key exchange algorithm explicitly e. g diffie-hellman-group14-sha1, diffie-hellman-group-exchange-sha256, diffie-hellman-group-exchange-sha1, diffie-hellman-group1-sha1."
    )
    Property<String> getKeyExchangeAlgorithm();
}
