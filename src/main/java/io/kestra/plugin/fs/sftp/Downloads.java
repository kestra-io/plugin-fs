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
    title = "Download multiple files via SFTP",
    description = "Lists matching files then downloads them to internal storage. Respects `maxFiles` (default 25) and optional post-download action (MOVE/DELETE). Defaults: port 22, user home as root, password auth unless a PEM key is provided, host key checking disabled by default."
)
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move them to an archive folder",
            full = true,
            code = """
                id: fs_sftp_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.sftp.Downloads
                    host: localhost
                    port: "22"
                    username: foo
                    password: "{{ secret('SFTP_PASSWORD') }}"
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        )
    }
)
public class Downloads extends io.kestra.plugin.fs.vfs.Downloads implements SftpInterface {
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> keyfile;
    @PluginProperty(secret = true, group = "advanced")
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
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> proxyUsername;
    @PluginProperty(secret = true, group = "connection")
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
