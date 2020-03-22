package org.kestra.task.fs;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.Task;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVfsTask extends Task {
    @InputProperty(
        description = "Hostname of the remote server",
        dynamic = true
    )
    protected String host;

    @InputProperty(
        description = "Port of the remote server",
        dynamic = true
    )
    protected String port;
}
