package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Trigger extends AbstractTrigger implements PollingTriggerInterface, AbstractVfsInterface, TriggerOutput<Downloads.Output> {
    @Schema(
        title = "The interval between test of triggers"
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected String host;
    protected String username;
    protected String password;

    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The action to do on find files"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Downloads.Action action;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    @PluginProperty(dynamic = true)
    private String moveDirectory;

    @Schema(
        title = "A regexp to filter on full path"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    @Schema(
        title = "List file recursively"
    )
    @Builder.Default
    private boolean recursive = false;

    abstract public String getPort();

    abstract protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException;

    abstract protected String scheme();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();
        URI from = VfsService.uri(
            runContext,
            this.scheme(),
            this.host,
            this.getPort(),
            this.username,
            this.password,
            this.from
        );

        // connection options
        FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            List.Output run;
            try {
                run = VfsService.list(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    from,
                    this.regExp,
                    this.recursive
                );
            } catch (FileNotFolderException fileNotFolderException) {
                logger.debug("From path doesn't exist '{}'", String.join(", ", fileNotFolderException.getInfo()));
                return Optional.empty();
            }

            if (run.getFiles().isEmpty()) {
                return Optional.empty();
            }

            java.util.List<File> files = run
                .getFiles()
                .stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .collect(Collectors.toList());


            java.util.List<File> list = files
                .stream()
                .map(throwFunction(file -> {
                    Download.Output download = VfsService.download(
                        runContext,
                        fsm,
                        fileSystemOptions,
                        VfsService.uri(
                            runContext,
                            this.scheme(),
                            this.host,
                            this.getPort(),
                            this.username,
                            this.password,
                            file.getPath().toString()
                        )
                    );

                    logger.debug("File '{}' download to '{}'", from.getPath(), download.getTo());

                    return file.withPath(download.getTo());
                }))
                .collect(Collectors.toList());

            if (this.action != null) {
                VfsService.archive(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    files,
                    this.action,
                    VfsService.uri(
                        runContext,
                        this.scheme(),
                        this.host,
                        this.getPort(),
                        this.username,
                        this.password,
                        this.moveDirectory
                    )
                );
            }

            ExecutionTrigger executionTrigger = ExecutionTrigger.of(
                this,
                Downloads.Output.builder().files(list).build()
            );

            Execution execution = Execution.builder()
                .id(runContext.getTriggerExecutionId())
                .namespace(context.getNamespace())
                .flowId(context.getFlowId())
                .flowRevision(context.getFlowRevision())
                .state(new State())
                .trigger(executionTrigger)
                .build();

            return Optional.of(execution);
        }
    }
}
