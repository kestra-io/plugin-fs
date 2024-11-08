package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVfsTask extends Task implements AbstractVfsInterface {
    protected Property<String> host;
    protected Property<String> username;
    protected Property<String> password;

    protected abstract FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException;
    protected abstract String scheme();

    protected URI uri(RunContext runContext, String filepath) throws IllegalVariableEvaluationException, URISyntaxException {
        return VfsService.uri(runContext, 
            this.scheme(),
            runContext.render(this.host).as(String.class).orElse(null),
            runContext.render(this.getPort()).as(String.class).orElse(null),
            runContext.render(this.username).as(String.class).orElse(null),
            runContext.render(this.password).as(String.class).orElse(null),
            filepath
        );
    }
}
