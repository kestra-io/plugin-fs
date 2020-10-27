package org.kestra.task.fs;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.Task;

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
    protected String host;

    @Schema(
        title = "Port of the remote server"
    )
    @PluginProperty(dynamic = true)
    protected String port;
}
