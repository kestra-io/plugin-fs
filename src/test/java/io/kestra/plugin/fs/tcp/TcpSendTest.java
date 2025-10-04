package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KestraTest(startWorker = true)
class TcpSendTest {
    private static int PORT;
    private static ServerSocket serverSocket;
    private static ExecutorService executor;
    private static final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void setup() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> serverReady = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                ServerSocket socket = new ServerSocket(0); // 0 = dynamic free port
                PORT = socket.getLocalPort();
                serverSocket = socket;

                serverReady.complete(null);

                while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                    try {
                        Socket client = serverSocket.accept();
                        handleClient(client);
                    } catch (IOException ignored) {}
                }
            } catch (IOException e) {
                serverReady.completeExceptionally(e);
            }
        });

        serverReady.get(5, TimeUnit.SECONDS);
    }

    private static void handleClient(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String message = in.readLine();
            if (message != null) {
                receivedMessages.add(message);
                out.println("ECHO:" + message);
            }
        } catch (IOException ignored) {}
    }

    @AfterAll
    static void teardown() throws Exception {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void testTcpSend() throws Exception {
        String payload = "Hello from TcpSend!";

        TcpSend task = TcpSend.builder()
            .id("tcp-send-task")
            .type(TcpSend.class.getSimpleName())
            .host("127.0.0.1")
            .port(PORT)
            .payload(payload + "\n") // newline for BufferedReader.readLine()
            .timeoutMs(2000)
            .build();

        // Mock RunContext
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        TcpSend.Output output = task.run(runContext);

        // Verify output
        assertEquals(PORT, output.getPort());
        assertEquals(payload.length() + 1, output.getSentBytes());

        // Verify server received it
        String received = receivedMessages.poll(2, TimeUnit.SECONDS);
        assertEquals(payload, received);
    }
}
