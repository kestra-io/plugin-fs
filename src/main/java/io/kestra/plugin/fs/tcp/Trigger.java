package io.kestra.plugin.fs.tcp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.realtime.RealtimeTrigger;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

@Setter
@SuperBuilder
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
@Plugin(
    examples = {
        @Example(
            title = "Listen for TCP messages on port 5000",
            code = """
                id: tcp_trigger_flow
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.debug.Return
                    format: "Received TCP: {{ trigger.message }}"

                triggers:
                  - id: tcp
                    type: io.kestra.plugin.fs.tcp.TcpRealtimeTrigger
                    port: 5000
                """
        )
    }
)
public class Trigger extends RealtimeTrigger<Trigger.Output> {
    @PluginProperty
    private Integer port;

    public Trigger() {
        super();
    }

    @Override
    protected void listen(RunContext runContext,
                          TriggerContext triggerContext,
                          ConditionContext conditionContext) {
        runContext.logger().info("TCP RealtimeTrigger listening on port {}", port);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (!Thread.currentThread().isInterrupted()) {
                Socket client = serverSocket.accept();
                runContext.logger().debug("New TCP client: {}", client.getRemoteSocketAddress());

                // spawn a client handler
                new Thread(() -> handleClient(runContext, triggerContext, conditionContext, client)).start();
            }
        } catch (Exception e) {
            runContext.logger().error("TCP Trigger error", e);
        }
    }

    void handleClient(RunContext runContext,
                      TriggerContext triggerContext,
                      ConditionContext conditionContext,
                      Socket client) {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8))) {

            StringBuilder message = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                message.append(line).append("\n");
            }

            emitEvent(runContext, conditionContext, triggerContext, Output.builder()
                .port(port)
                .message(message.toString().trim())
                .remoteAddress(client.getInetAddress().getHostAddress())
                .build());

        } catch (Exception e) {
            runContext.logger().error("Error handling TCP client", e);
        }
    }

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        return Flux.create(emitter -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (!Thread.currentThread().isInterrupted()) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> {
                        try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8))) {

                            StringBuilder message = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                message.append(line).append("\n");
                            }

                            Output output = Output.builder()
                                .port(port)
                                .message(message.toString().trim())
                                .remoteAddress(client.getRemoteSocketAddress().toString())
                                .build();

                            emitter.next(TriggerService.generateRealtimeExecution(
                                this,
                                conditionContext,
                                context,
                                output
                            ));

                        } catch (Exception e) {
                            emitter.error(e);
                        }
                    }).start();
                }
            } catch (Exception e) {
                emitter.error(e);
            }
        });
    }


    @Builder
    @Getter
    @ToString
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final Integer port;
        private final String message;
        private final String remoteAddress;
    }
}
