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
    title = "Download a file from an SFTP server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_sftp_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.fs.sftp.Download
                    host: localhost
                    port: "22"
                    username: foo
                    password: "{{ secret('SFTP_PASSWORD') }}"
                    from: "/in/file.txt"
                """
        )
    }
)
public class Download extends io.kestra.plugin.fs.vfs.Download implements SftpInterface {
    protected Property<String> keyfile;
    protected Property<String> passphrase;
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<String> proxyUser;
    protected Property<String> proxyPassword;
    protected Property<String> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.ofValue(true);
    @Builder.Default
    protected Property<String> port = Property.ofValue("22");
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
