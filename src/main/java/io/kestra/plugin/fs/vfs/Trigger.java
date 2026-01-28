package io.kestra.plugin.fs.vfs;

import com.jcraft.jsch.JSch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, AbstractVfsInterface, TriggerOutput<Downloads.Output> {

    @Schema(title = "The interval between test of triggers")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected Property<String> host;
    protected Property<String> username;
    protected Property<String> password;

    @Schema(title = "The directory to list")
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering."
    )
    @NotNull
    private Property<Downloads.Action> action;

    @Schema(title = "The destination directory in case off `MOVE` ")
    private Property<String> moveDirectory;

    @Schema(title = "A regexp to filter on full path")
    private Property<String> regExp;

    @Schema(title = "List file recursively")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Enable the RSA/SHA1 algorithm (disabled by default)")
    @NotNull
    private Property<Boolean> enableSshRsa1 = Property.ofValue(false);

    @Schema(title = "The maximum number of files to retrieve at once")
    private Property<Integer> maxFiles;

    protected abstract FileSystemOptions fsOptions(RunContext runContext)
        throws IllegalVariableEvaluationException, IOException;

    protected abstract String scheme();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context)
        throws Exception {

        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();
        URI fromUri = createUri(runContext);

        FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

        var renderedHost = runContext.render(this.host).as(String.class).orElseThrow();
        var renderedPort = runContext.render(this.getPort()).as(String.class).orElseThrow();

        var jsch = new JSch();
        var session = jsch.getSession(
            runContext.render(username).as(String.class).orElse(null),
            renderedHost,
            Integer.parseInt(renderedPort)
        );

        if (runContext.render(enableSshRsa1).as(Boolean.class).orElseThrow()) {
            logger.info("RSA/SHA1 is enabled, be advised that SHA1 is no longer considered secure.");
            session.setConfig("server_host_key", session.getConfig("server_host_key") + ",ssh-rsa");
            session.setConfig(
                "PubkeyAcceptedAlgorithms",
                session.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa"
            );
        }

        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(
                StandardFileSystemManager.class.getResource(
                    KestraStandardFileSystemManager.CONFIG_RESOURCE
                )
            );
            fsm.init();

            List.Output listed;
            try {
                listed = VfsService.list(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    fromUri,
                    runContext.render(this.regExp).as(String.class).orElse(null),
                    runContext.render(this.recursive).as(Boolean.class).orElse(false)
                );
            } catch (FileNotFolderException e) {
                logger.debug("From path doesn't exist '{}'", String.join(", ", e.getInfo()));
                return Optional.empty();
            }

            if (listed.getFiles().isEmpty()) {
                return Optional.empty();
            }

            java.util.List<File> files = listed.getFiles()
                .stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .toList();

            if (files.isEmpty()) {
                return Optional.empty();
            }

            Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(null);
            java.util.List<File> selectedFiles = files;

            if (rMaxFiles != null && files.size() > rMaxFiles) {
                logger.warn(
                    "Too many files to process ({}), limiting to {}",
                    files.size(),
                    rMaxFiles
                );
                selectedFiles = files.subList(0, rMaxFiles);
            }

            java.util.List<File> downloadedFiles = selectedFiles
                .stream()
                .map(throwFunction(file -> {
                    Download.Output download = VfsService.download(
                        runContext,
                        fsm,
                        fileSystemOptions,
                        VfsService.uri(
                            runContext,
                            this.scheme(),
                            runContext.render(this.host).as(String.class).orElse(null),
                            runContext.render(this.getPort()).as(String.class).orElse(null),
                            runContext.render(this.username).as(String.class).orElse(null),
                            runContext.render(this.password).as(String.class).orElse(null),
                            file.getServerPath().getPath()
                        )
                    );

                    logger.debug(
                        "File '{}' downloaded to '{}'",
                        file.getServerPath(),
                        download.getTo()
                    );

                    return file.withPath(download.getTo());
                }))
                .toList();

            if (this.action != null) {
                VfsService.performAction(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    selectedFiles,
                    runContext.render(this.action)
                        .as(Downloads.Action.class)
                        .orElse(null),
                    VfsService.uri(
                        runContext,
                        this.scheme(),
                        runContext.render(this.host).as(String.class).orElse(null),
                        runContext.render(this.getPort()).as(String.class).orElse(null),
                        runContext.render(this.username).as(String.class).orElse(null),
                        runContext.render(this.password).as(String.class).orElse(null),
                        runContext.render(this.moveDirectory).as(String.class).orElse(null)
                    )
                );
            }

            Execution execution = TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Downloads.Output.builder()
                    .files(downloadedFiles)
                    .build()
            );

            return Optional.of(execution);
        }
    }

    private URI createUri(RunContext runContext)
        throws IllegalVariableEvaluationException, URISyntaxException {

        return VfsService.uri(
            runContext,
            this.scheme(),
            runContext.render(this.host).as(String.class).orElse(null),
            runContext.render(this.getPort()).as(String.class).orElse(null),
            runContext.render(this.username).as(String.class).orElse(null),
            runContext.render(this.password).as(String.class).orElse(null),
            runContext.render(this.from).as(String.class).orElseThrow()
        );
    }
}
