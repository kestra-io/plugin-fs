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
        title = "Is the path relative to the users home directory"
    )
    @PluginProperty(dynamic = false)
    Boolean getRootDir();

    @Schema(
        title = "Whether to use a [passive mode](https://www.jscape.com/blog/active-v-s-passive-ftp-simplified). Passive mode is generally considered more secure as it's less likely to encounter issues with NAT and firewalls. Therefore, this property is by default set to `true`. To use active mode instead, set the property to `false`."
    )
    @PluginProperty(dynamic = false)
    Boolean getPassiveMode();

    @Schema(
        title = "Control that the server ip that emit the request is the same than send response."
    )
    @PluginProperty(dynamic = false)
    Boolean getRemoteIpVerification();
}
