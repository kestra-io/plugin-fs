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
        title = "Upload files to an SFTP server's directory."
)
@Plugin(
        examples = {
                @Example(
                        full = true,
                        code = """
                            id: fs_sftp_uploads
                            namespace: company.team

                            inputs:
                              - id: file1
                                type: FILE
                              - id: file2
                                type: FILE
            
                            tasks:
                              - id: uploads
                                type: io.kestra.plugin.fs.sftp.Uploads
                                host: localhost
                                port: "22"
                                username: foo
                                password: "{{ secret('SFTP_PASSWORD') }}"
                                from:
                                  - "{{ inputs.file1 }}"
                                  - "{{ inputs.file2 }}"
                                to: "/upload/dir2"
                            """
                )
        }
)
public class Uploads extends io.kestra.plugin.fs.vfs.Uploads implements SftpInterface {
    protected Property<String> keyfile;
    protected Property<String> passphrase;
    @Deprecated
    protected Property<String> proxyHost;
    protected Property<String> proxyAddress;
    protected Property<String> proxyPort;
    @Deprecated
    protected Property<String> proxyUser;
    protected Property<String> proxyUsername;
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
