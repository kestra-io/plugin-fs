package org.kestra.task.fs.sftp;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.*;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.annotations.PluginProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download file from sftp server"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 6622",
                "username: foo",
                "password: pass",
                "from: \"/in/file.txt\"",
            }
        )
    }
)
public class Download extends AbstractSftpTask implements RunnableTask<SftpOutput> {
    @Schema(
        title = "The fully-qualified URIs that point to destination path"
    )
    @PluginProperty(dynamic = true)
    protected String from;

    public SftpOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = new URI(this.sftpUri(runContext, this.from));

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

        // download
        try {
            File tempFile = download(fsm, fsOptionWithCleanUp.getOptions(), from);

            URI storageUri = runContext.putTempFile(tempFile);

            logger.debug("File '{}' download to '{}'", from, storageUri);

            return SftpOutput.builder()
                .from(from)
                .to(storageUri)
                .build();
        } finally {
            fsOptionWithCleanUp.getCleanup().run();
        }
    }

    static File download(FileSystemManager fsm, FileSystemOptions fileSystemOptions, URI from) throws IOException {
        // temp file where download will be copied
        File tempFile = File.createTempFile(
            Download.class.getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        try (
            FileObject local = fsm.resolveFile(tempFile.toURI());
            FileObject remote = fsm.resolveFile(from.toString(), fileSystemOptions)
        ) {
            local.copyFrom(remote, Selectors.SELECT_SELF);
        }

        return tempFile;

    }
}
