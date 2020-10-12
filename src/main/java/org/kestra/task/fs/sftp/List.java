package org.kestra.task.fs.sftp;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.provider.sftp.SftpFileObject;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.kestra.task.fs.sftp.models.File;
import org.slf4j.Logger;

import java.net.URI;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.RegEx;

import static org.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "List files in a sftp server directory"
)
public class List extends AbstractSftpTask implements RunnableTask<List.Output> {
    @InputProperty(
        description = "The fully-qualified URIs that point to path",
        dynamic = true
    )
    protected String from;

    @InputProperty(
        description = "A regexp to filter on full path"
    )
    @RegEx
    private String regExp;

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        //noinspection resource never close the global instance
        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = new URI(this.sftpUri(runContext, this.from));

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

        // list
        try {
            try (
                FileObject local = fsm.resolveFile(from.toString(), fsOptionWithCleanUp.getOptions())
            ) {
                FileObject[] children = local.getChildren();

                java.util.List<File> list = Stream.of(children)
                    .map(throwFunction(r -> File.of((SftpFileObject) r)))
                    .filter(r -> regExp == null || r.getPath().toString().matches(this.regExp))
                    .collect(Collectors.toList());

                logger.debug("Found '{}' files from '{}'", list.size(), from);

                return Output.builder()
                    .files(list)
                    .build();
            }
        } finally {
            fsOptionWithCleanUp.getCleanup().run();
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The list of files"
        )
        private final java.util.List<File> files;
    }
}
