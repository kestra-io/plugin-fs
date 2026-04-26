package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import io.kestra.core.models.annotations.PluginProperty;

public interface AbstractVfsInterface {
    @Schema(
        title = "Remote host"
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<String> getHost();

    @Schema(
        title = "Remote port"
    )
    @PluginProperty(group = "connection")
    Property<String> getPort();

    @Schema(
        title = "Username"
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Password"
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();
}
