package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;
import java.net.Proxy;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload a file to a FTP server"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_ftp_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.fs.ftp.Upload
                    host: localhost
                    port: 21
                    username: foo
                    password: pass
                    from: "{{ inputs.file }}"
                    to: "/upload/dir2/file.txt"
                """
        )
    }
)
public class Upload extends io.kestra.plugin.fs.vfs.Upload implements FtpInterface {
    protected String proxyHost;
    protected String proxyPort;
    protected Proxy.Type proxyType;
    @Builder.Default
    protected Boolean rootDir = true;
    @Builder.Default
    protected String port = "21";
    @Builder.Default
    protected Boolean passiveMode = true;
    @Builder.Default
    protected Boolean remoteIpVerification = true;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
