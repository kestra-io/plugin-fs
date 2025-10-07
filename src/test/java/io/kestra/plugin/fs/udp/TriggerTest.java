package io.kestra.plugin.fs.udp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KestraTest(startWorker = true)
class TriggerTest {

    private static final int PORT = 6021;

    private static final CountDownLatch serverStarted = new CountDownLatch(1);
    private static final List<Trigger.Output> emittedOutputs = new ArrayList<>();

    /**
     * Custom subclass that captures emitted outputs
     */
    static class TestUdpRealtimeTrigger extends Trigger {

        private final List<Output> captured;
        private final CountDownLatch startedSignal;

        TestUdpRealtimeTrigger(int port, List<Output> captured, CountDownLatch startedSignal) {
            this.setPort(port);
            this.captured = captured;
            this.startedSignal = startedSignal;
        }

        @Override
        protected void listen(RunContext runContext,
                              TriggerContext triggerContext,
                              ConditionContext conditionContext) {
            try (DatagramSocket socket = new DatagramSocket(PORT)) {
                startedSignal.countDown(); // signal that UDP socket is bound
                byte[] buffer = new byte[4096];

                while (!Thread.currentThread().isInterrupted()) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    emitEvent(runContext, conditionContext, triggerContext, Output.builder()
                        .port(PORT)
                        .message(message.trim())
                        .remoteAddress(packet.getAddress().getHostAddress())
                        .build());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void emitEvent(RunContext runContext,
                              ConditionContext conditionContext,
                              TriggerContext triggerContext,
                              Output output) {
            captured.add(output);
        }
    }

    @Test
    void testTriggerReceivesUdpMessage() throws Exception {
        // Start trigger in a background thread
        TestUdpRealtimeTrigger trigger = new TestUdpRealtimeTrigger(PORT, emittedOutputs, serverStarted);
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                trigger.start(null, new TriggerContext(), new ConditionContext());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Wait for UDP socket to start
        if (!serverStarted.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("UDP Trigger server did not start in time");
        }

        // Send UDP message to trigger
        try (DatagramSocket socket = new DatagramSocket()) {
            byte[] payload = "Hello UDP Trigger!".getBytes(StandardCharsets.UTF_8);
            InetAddress address = InetAddress.getByName("127.0.0.1");
            DatagramPacket packet = new DatagramPacket(payload, payload.length, address, PORT);
            socket.send(packet);
        }

        // Allow trigger time to process
        Thread.sleep(500);

        // Assertions
        assertEquals(1, emittedOutputs.size());
        Trigger.Output output = emittedOutputs.get(0);
        assertEquals(PORT, output.getPort());
        assertEquals("Hello UDP Trigger!", output.getMessage());
        assertEquals("127.0.0.1", output.getRemoteAddress());
    }
}
