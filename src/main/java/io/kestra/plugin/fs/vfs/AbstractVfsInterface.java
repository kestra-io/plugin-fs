package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface AbstractVfsInterface {
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
        title = "Username"
    )
    Property<String> getUsername();

    @Schema(
        title = "Password"
    )
    Property<String> getPassword();
}
