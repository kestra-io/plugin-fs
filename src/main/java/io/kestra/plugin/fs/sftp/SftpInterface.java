package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public interface SftpInterface {
    @Schema(
        title = "Private keyfile in the PEM file format to connect to a remote server using SSH",
        description = "To generate a PEM format key from OpenSSH, use the following command: `ssh-keygen -m PEM`"
    )
    @PluginProperty(dynamic = true)
    String getKeyfile();

    @Schema(
        title = "Passphrase of the ssh key"
    )
    @PluginProperty(dynamic = true)
    String getPassphrase();

    @Schema(
        title = "SFTP proxy host"
    )
    @PluginProperty(dynamic = true)
    String getProxyHost();

    @Schema(
        title = "SFTP proxy port"
    )
    @PluginProperty(dynamic = true)
    String getProxyPort();

    @Schema(
        title = "SFTP proxy user"
    )
    @PluginProperty(dynamic = true)
    String getProxyUser();

    @Schema(
        title = "SFTP proxy password"
    )
    @PluginProperty(dynamic = true)
    String getProxyPassword();

    @Schema(
        title = "SFTP proxy type"
    )
    @PluginProperty(dynamic = true)
    String getProxyType();

    @Schema(
        title = "Is path is relative to root dir"
    )
    @PluginProperty(dynamic = true)
    Boolean getRootDir();
}
