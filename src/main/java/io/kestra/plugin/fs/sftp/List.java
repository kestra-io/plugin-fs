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
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List files in an SFTP directory",
    description = "Lists entries under the given path with optional regexp filter. Defaults: port 22, user home as root, password auth unless a PEM key is provided, host key checking disabled by default."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_sftp_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.fs.sftp.List
                    host: localhost
                    port: "22"
                    username: foo
                    password: "{{ secret('SFTP_PASSWORD') }}"
                    from: "/upload/dir1/"
                    regExp: ".*\\/dir1\\/.*.(yaml|yml)"
                """
        )
    }
)
public class List extends io.kestra.plugin.fs.vfs.List implements SftpInterface {
    @PluginProperty(group = "connection")
    protected Property<String> keyfile;
    @PluginProperty(group = "advanced")
    protected Property<String> passphrase;
    @Deprecated
    @PluginProperty(group = "deprecated")
    protected Property<String> proxyHost;
    @PluginProperty(group = "advanced")
    protected Property<String> proxyAddress;
    @PluginProperty(group = "connection")
    protected Property<String> proxyPort;
    @Deprecated
    @PluginProperty(group = "deprecated")
    protected Property<String> proxyUser;
    @PluginProperty(group = "connection")
    protected Property<String> proxyUsername;
    @PluginProperty(group = "connection")
    protected Property<String> proxyPassword;
    @PluginProperty(group = "advanced")
    protected Property<String> proxyType;
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> rootDir = Property.ofValue(true);
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<String> port = Property.ofValue("22");
    @PluginProperty(group = "connection")
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
