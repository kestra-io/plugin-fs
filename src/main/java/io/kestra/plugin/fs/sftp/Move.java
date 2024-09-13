package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Move a file to a SFTP server.",
    description ="If the destination directory doesn't exist, it will be created"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_sftp_move
                namespace: company.team

                tasks:
                  - id: move
                    type: io.kestra.plugin.fs.sftp.Move
                    host: localhost
                    port: "22"
                    username: foo
                    password: pass
                    from: "/upload/dir1/file.txt"
                    to: "/upload/dir2/file.txt"
                """
        )
    }
)
public class Move extends io.kestra.plugin.fs.vfs.Move implements SftpInterface {
    protected String keyfile;
    protected String passphrase;
    protected String proxyHost;
    protected String proxyPort;
    protected String proxyUser;
    protected String proxyPassword;
    protected String proxyType;
    @Builder.Default
    protected Boolean rootDir = true;
    @Builder.Default
    protected String port = "22";
    protected String keyExchangeAlgorithm;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SftpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "sftp";
    }
}
