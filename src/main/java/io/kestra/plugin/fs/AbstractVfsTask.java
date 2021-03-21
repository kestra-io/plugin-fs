package io.kestra.plugin.fs;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVfsTask extends Task {
    @Schema(
        title = "Hostname of the remote server"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String host;

    @Schema(
        title = "Port of the remote server"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected String port = "22";
}
