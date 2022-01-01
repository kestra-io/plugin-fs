package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
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
    protected String host;
    protected String username;
    protected String password;

    abstract public String getPort();
    abstract protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException;
    abstract protected String scheme();

    protected URI uri(RunContext runContext, String filepath) throws IllegalVariableEvaluationException, URISyntaxException {
        return VfsService.uri(runContext, this.scheme(), this.host, this.getPort(), this.username, this.password, filepath);
    }
}
