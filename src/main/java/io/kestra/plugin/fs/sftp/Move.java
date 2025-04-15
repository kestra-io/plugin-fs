package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    title = "Move a file to an SFTP server.",
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
    protected Property<String> keyfile;
    protected Property<String> passphrase;
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<String> proxyUser;
    protected Property<String> proxyPassword;
    protected Property<String> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.of(true);
    @Builder.Default
    protected Property<String> port = Property.of("22");
    protected Property<String> keyExchangeAlgorithm;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SftpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "sftp";
    }
}
