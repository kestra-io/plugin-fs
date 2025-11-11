package io.kestra.plugin.fs.tcp;

import jakarta.inject.Singleton;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;



@Singleton
public class TcpService {

    private static TcpService instance;

    private TcpService() {}

    public static TcpService getInstance() {
        if (instance == null) {
            instance = new TcpService();
        }
        return instance;
    }

    public Socket connect(String host, int port, Duration timeout) throws IOException {
        Socket socket = new Socket();
        if (timeout != null) {
            socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
            socket.setSoTimeout((int) timeout.toMillis());
        } else {
            socket.connect(new InetSocketAddress(host, port));
        }
        return socket;
    }

    public byte[] encodePayload(String payload, String encoding) {
        if ("BASE64".equalsIgnoreCase(encoding)) {
            return Base64.getDecoder().decode(payload);
        }
        Charset charset = Charset.forName(encoding);
        return payload.getBytes(charset);
    }

    public String decodePayload(byte[] bytes, String encoding) {
        if ("BASE64".equalsIgnoreCase(encoding)) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        Charset charset = Charset.forName(encoding);
        return new String(bytes, charset);
    }
}
