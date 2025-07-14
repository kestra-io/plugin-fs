package io.kestra.plugin.fs.ftp;

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
import java.net.Proxy;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
        title = "Upload files to an FTP server's directory."
)
@Plugin(
        examples = {
                @Example(
                        full = true,
                        code = """
                            id: fs_ftp_uploads
                            namespace: company.team

                            inputs:
                              - id: file1
                                type: FILE
                              - id: file2
                                type: FILE

                            tasks:
                              - id: uploads
                                type: io.kestra.plugin.fs.ftp.Uploads
                                host: localhost
                                port: 21
                                username: foo
                                password: "{{ secret('FTP_PASSWORD') }}"
                                from:
                                  - "{{ inputs.file1 }}"
                                  - "{{ inputs.file2 }}"
                                to: "/upload/dir2"
                            """
                )
        }
)
public class Uploads extends io.kestra.plugin.fs.vfs.Uploads implements FtpInterface {
    private Property<String> proxyHost;
    private Property<String> proxyPort;
    private Property<Proxy.Type> proxyType;
    @Builder.Default
    private Property<Boolean> rootDir = Property.ofValue(true);
    @Builder.Default
    private Property<String> port = Property.ofValue("21");
    @Builder.Default
    private Property<Boolean> passiveMode = Property.ofValue(true);
    @Builder.Default
    private Property<Boolean> remoteIpVerification = Property.ofValue(true);
    @Builder.Default
    protected Options options = Options.builder().build();

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
