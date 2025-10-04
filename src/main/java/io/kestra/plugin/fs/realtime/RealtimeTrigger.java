package io.kestra.plugin.fs.realtime;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.triggers.*;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuperBuilder
@NoArgsConstructor
public abstract class RealtimeTrigger<T extends Output>
    extends AbstractTrigger implements RealtimeTriggerInterface {

    private transient ExecutorService executor;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    public void start(RunContext runContext, TriggerContext triggerContext, ConditionContext conditionContext) throws Exception {
        if (executor == null || executor.isShutdown()) {
            executor = Executors.newSingleThreadExecutor();
        }

        executor.submit(() -> {
            try {
                this.listen(runContext, triggerContext, conditionContext);
            } catch (Exception e) {
                runContext.logger().error("Realtime trigger failed", e);
            }
        });
    }

    @Override
    public void stop() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdownNow();
        }
        try {
            this.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void listen(RunContext runContext,
                                   TriggerContext triggerContext,
                                   ConditionContext conditionContext) throws Exception;

    protected void close() throws Exception {
        // no-op
    }

    public void emitEvent(RunContext runContext,
                             ConditionContext conditionContext,
                             TriggerContext triggerContext,
                             T output) throws QueueException {
        Execution execution = TriggerService.generateRealtimeExecution(
            this,
            conditionContext,
            triggerContext,
            output
        );

        runContext.logger().info("RealtimeTrigger [{}] emitted execution: {}", this.getId(), execution.getId());

        executionQueue.emit(execution);
    }
}
