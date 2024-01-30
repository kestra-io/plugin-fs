package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotNull;

public interface AbstractVfsInterface {
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
        title = "Username on the remote server"
    )
    @PluginProperty(dynamic = true)
    String getUsername();

    @Schema(
        title = "Password on the remote server"
    )
    @PluginProperty(dynamic = true)
    String getPassword();
}
