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
        title = "Upload files to a SFTP server's directory"
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
                                password: pass
                                from:
                                  - "{{ inputs.file1 }}"
                                  - "{{ inputs.file2 }}"
                                to: "/upload/dir2"
                            """
                )
        }
)
public class Uploads extends io.kestra.plugin.fs.vfs.Uploads implements SftpInterface {
    private String keyfile;
    private String passphrase;
    private String proxyHost;
    private String proxyPort;
    private String proxyUser;
    private String proxyPassword;
    private String proxyType;
    @Builder.Default
    private Boolean rootDir = true;
    @Builder.Default
    private String port = "22";
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
