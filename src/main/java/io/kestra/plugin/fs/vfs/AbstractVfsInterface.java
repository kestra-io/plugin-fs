package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface AbstractVfsInterface {
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
        title = "Username on the remote server"
    )
    Property<String> getUsername();

    @Schema(
        title = "Password on the remote server"
    )
    Property<String> getPassword();
}
