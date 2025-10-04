package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.runners.RunContext;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KestraTest(startWorker = true)
class TcpRealtimeTriggerTest {

    private static final int PORT = 6020;

    private static final CountDownLatch serverStarted = new CountDownLatch(1);
    private static final List<TcpRealtimeTrigger.Output> emittedOutputs = new ArrayList<>();

    /**
     * Custom subclass that captures emitted outputs
     */
    static class TestTcpRealtimeTrigger extends TcpRealtimeTrigger {

        private final List<Output> captured;
        private final CountDownLatch startedSignal;

        TestTcpRealtimeTrigger(int port, List<Output> captured, CountDownLatch startedSignal) {
            this.setPort(port);
            this.captured = captured;
            this.startedSignal = startedSignal;
        }

        @Override
        protected void listen(RunContext runContext,
                              TriggerContext triggerContext,
                              ConditionContext conditionContext) {
            try (var serverSocket = new java.net.ServerSocket(PORT)) {
                // Signal that server socket is bound
                startedSignal.countDown();

                while (!Thread.currentThread().isInterrupted()) {
                    var client = serverSocket.accept();
                    new Thread(() -> handleClient(runContext, triggerContext, conditionContext, client)).start();
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

        @Override
        protected void handleClient(RunContext runContext,
                                    TriggerContext triggerContext,
                                    ConditionContext conditionContext,
                                    Socket client) {
            try (var reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8))) {

                StringBuilder message = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    message.append(line).append("\n");
                }

                emitEvent(runContext, conditionContext, triggerContext, Output.builder()
                    .port(PORT)
                    .message(message.toString().trim())
                    .remoteAddress(client.getInetAddress().getHostAddress()) // reliable IP
                    .build());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testTriggerReceivesMessage() throws Exception {
        // Create and start the trigger in a separate thread
        TestTcpRealtimeTrigger trigger = new TestTcpRealtimeTrigger(PORT, emittedOutputs, serverStarted);
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                trigger.start(null, new TriggerContext(), new ConditionContext());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Wait for server to start
        if (!serverStarted.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Trigger server did not start in time");
        }

        // Connect to the trigger server and send a message
        try (Socket socket = new Socket("127.0.0.1", PORT);
             OutputStream out = socket.getOutputStream()) {

            String payload = "Hello TCP Trigger!\n"; // newline required
            out.write(payload.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }

        // Give trigger some time to process
        Thread.sleep(500);

        // Assertions
        assertEquals(1, emittedOutputs.size());
        TcpRealtimeTrigger.Output output = emittedOutputs.get(0);
        assertEquals(PORT, output.getPort());
        assertEquals("Hello TCP Trigger!", output.getMessage());
        assertEquals("127.0.0.1", output.getRemoteAddress());
    }
}
