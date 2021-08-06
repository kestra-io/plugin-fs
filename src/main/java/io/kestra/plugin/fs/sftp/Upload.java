package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.*;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload a file to a sftp server"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 6622",
                "username: foo",
                "password: pass",
                "from: \"{{ outputs.taskid.uri }}\"",
                "to: \"/upload/dir2/file.txt\"",
            }
        )
    }
)
public class Upload extends AbstractSftpTask implements RunnableTask<SftpOutput> {
    @Schema(
        title = "The file path to copy"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The destination path"
    )
    @PluginProperty(dynamic = true)
    private String to;

    public SftpOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        FileSystemManager fsm = VFS.getManager();

        // from & to
        String toPath = runContext.render(this.to);
        URI to = this.sftpUri(runContext, toPath);
        URI from = new URI(runContext.render(this.from));

        // copy from to a temp files
        File tempFile = runContext.tempFile().toFile();

        // copy from to a temp file
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.uriToInputStream(from), outputStream);
        }

        // connection options
        FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

        // upload
        try (FileObject local = fsm.resolveFile(tempFile.toURI());
             FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions);
        ) {
            remote.copyFrom(local, Selectors.SELECT_SELF);
        }

        logger.debug("File '{}' uploaded to '{}'", from, to.getPath());

        return SftpOutput.builder()
            .from(from)
            .to(to)
            .build();
    }
}
