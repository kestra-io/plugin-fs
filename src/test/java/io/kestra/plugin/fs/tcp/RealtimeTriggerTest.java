package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class RealtimeTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldTriggerOnTcpMessage() throws Exception {
        int port = 9123;
        RunContext runContext = runContextFactory.of();

        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .flow(Flow.builder().id("tcp_realtime_test").namespace("dev").build())
            .build();

        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("dev")
            .flowId("tcp_realtime_test")
            .triggerId("tcp_listener")
            .build();

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue(port))
            .encoding(Property.ofValue("UTF-8"))
            .build();

        Future<Execution> listener = Executors.newSingleThreadExecutor().submit(() -> {
            Flux<Execution> flux = Flux.from(trigger.evaluate(conditionContext, triggerContext))
                .subscribeOn(Schedulers.boundedElastic());
            return flux.blockFirst(Duration.ofSeconds(8));
        });

        waitForPort("127.0.0.1", port, Duration.ofSeconds(4));

        sendTestMessage(port, "Hello from Kestra TCP Trigger");

        Execution execution = listener.get(); 
        assertThat(execution, notNullValue());

        Map<String, Object> vars = execution.getTrigger().getVariables();
        assertThat(vars.get("payload"), is("Hello from Kestra TCP Trigger"));
        assertThat(vars.get("sourceIp"), notNullValue());
        assertThat(vars.get("timestamp"), notNullValue());

        trigger.stop();
    }

    private void sendTestMessage(int port, String message) {
        try (Socket socket = new Socket("127.0.0.1", port);
             OutputStream os = socket.getOutputStream()) {
            os.write((message + "\n").getBytes(StandardCharsets.UTF_8));
            os.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForPort(String host, int port, Duration timeout) throws Exception {
        long end = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < end) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 200);
                return;
            } catch (Exception ignored) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("Timeout waiting for port " + port + " to open");
    }
}
