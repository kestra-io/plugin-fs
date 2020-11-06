package org.kestra.task.fs.sftp;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

    @SuppressWarnings({"CaughtExceptionImmediatelyRethrown"})
    public SftpOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        FileSystemManager fsm = VFS.getManager();

        // from & to
        String toPath = runContext.render(this.to);
        URI to = this.sftpUri(runContext, toPath);
        URI from = new URI(runContext.render(this.from));

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        // copy from to a temp file
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.uriToInputStream(from), outputStream);
        }

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

        // upload
        try {
            try (FileObject local = fsm.resolveFile(tempFile.toURI());
                 FileObject remote = fsm.resolveFile(to.toString(), fsOptionWithCleanUp.getOptions());
            ) {
                remote.copyFrom(local, Selectors.SELECT_SELF);
            }

            logger.debug("File '{}' uploaded to '{}'", from, to);

            return SftpOutput.builder()
                .from(from)
                .to(to)
                .build();
        } catch (IOException error) {
            throw error;
        } finally {
            fsOptionWithCleanUp.getCleanup().run();
        }
    }
}
