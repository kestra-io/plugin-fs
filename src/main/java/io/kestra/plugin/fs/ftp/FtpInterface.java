package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.net.Proxy;

public interface FtpInterface {
    @Schema(
        title = "FTP proxy host"
    )
    @PluginProperty(dynamic = true)
    String getProxyHost();

    @Schema(
        title = "FTP proxy port"
    )
    @PluginProperty(dynamic = true)
    String getProxyPort();

    @Schema(
        title = "FTP proxy type"
    )
    @PluginProperty(dynamic = true)
    Proxy.Type getProxyType();

    @Schema(
        title = "Is path is relative to root dir"
    )
    @PluginProperty(dynamic = false)
    Boolean getRootDir();

    @Schema(
        title = "Enter into passive mode."
    )
    @PluginProperty(dynamic = false)
    Boolean getPassiveMode();
}
