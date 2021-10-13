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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
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

        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            // path
            URI from = this.sftpUri(runContext, this.from);

            // connection options
            FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

            // download
            File tempFile = download(fsm, fileSystemOptions, from, runContext);

            URI storageUri = runContext.putTempFile(tempFile);

            logger.debug("File '{}' download to '{}'", from.getPath(), storageUri);

            return SftpOutput.builder()
                .from(from)
                .to(storageUri)
                .build();
        }
    }

    static File download(FileSystemManager fsm, FileSystemOptions fileSystemOptions, URI from, RunContext runContext) throws IOException {
        // temp file where download will be copied
        File tempFile = runContext.tempFile().toFile();

        try (
            FileObject local = fsm.resolveFile(tempFile.toURI());
            FileObject remote = fsm.resolveFile(from.toString(), fileSystemOptions)
        ) {
            local.copyFrom(remote, Selectors.SELECT_SELF);
        }

        return tempFile;

    }
}
