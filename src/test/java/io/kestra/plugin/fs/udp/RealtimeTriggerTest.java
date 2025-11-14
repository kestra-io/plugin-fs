package io.kestra.plugin.fs.udp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class RealtimeTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldTriggerOnUdpMessage() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        int testPort = 9300 + (int) (Math.random() * 100);
        String message = "Hello UDP Trigger";

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .host(Property.ofValue("0.0.0.0"))
            .port(Property.ofValue(testPort))
            .build();

        ConditionContext conditionContext = new ConditionContext(null, null, runContext, Map.of(), null);
        TriggerContext triggerContext = TriggerContext.builder().build();

        Thread triggerThread = new Thread(() -> {
            try {
                trigger.evaluate(conditionContext, triggerContext)
                    .subscribe(new org.reactivestreams.Subscriber<Execution>() {
                        @Override
                        public void onSubscribe(org.reactivestreams.Subscription s) {
                            s.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(Execution execution) {
                            RealtimeTrigger.Output output = (RealtimeTrigger.Output)
                                execution.getTrigger().getVariables().get("output");
                            assertThat(output.getPayload(), is(message));
                            assertThat(output.getSourceIp(), notNullValue());
                            assertThat(output.getTimestamp(), notNullValue());
                        }

                        @Override
                        public void onError(Throwable t) {
                            throw new RuntimeException(t);
                        }

                        @Override
                        public void onComplete() {}
                    });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        triggerThread.start();
        Thread.sleep(1000); // wait for trigger to start listening

        // Send UDP packet
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] data = message.getBytes();
            DatagramPacket packet = new DatagramPacket(
                data, data.length, InetAddress.getByName("127.0.0.1"), testPort
            );
            socket.send(packet);
        }

        Thread.sleep(1000);
        trigger.stop();
    }
}
