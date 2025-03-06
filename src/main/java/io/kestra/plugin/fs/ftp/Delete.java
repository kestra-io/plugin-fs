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

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a file to a FTP server."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_ftp_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.fs.ftp.Delete
                    host: localhost
                    port: 21
                    username: foo
                    password: pass
                    uri: "/upload/dir1/file.txt"
                """
        )
    }
)
public class Delete extends io.kestra.plugin.fs.vfs.Delete implements FtpInterface {
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<Proxy.Type> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.of(true);
    @Builder.Default
    protected Property<String> port = Property.of("21");
    @Builder.Default
    protected Property<Boolean> passiveMode = Property.of(true);
    @Builder.Default
    protected Property<Boolean> remoteIpVerification = Property.of(true);
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
