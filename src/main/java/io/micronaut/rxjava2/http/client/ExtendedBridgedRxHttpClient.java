package io.micronaut.rxjava2.http.client;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.StreamingHttpClient;

import java.net.URL;

/**
 * @see <a href="https://github.com/micronaut-projects/micronaut-rxjava2/pull/51">Waiting for release</a>
 */
public class ExtendedBridgedRxHttpClient extends BridgedRxStreamingHttpClient {
    private final StreamingHttpClient httpClient;

    ExtendedBridgedRxHttpClient(StreamingHttpClient httpClient) {
        super(httpClient);
        this.httpClient = httpClient;
    }

    @Override
    public void close() {
        httpClient.close();
    }

    @Override
    @NonNull
    public HttpClient start() {
        httpClient.start();
        return this;
    }

    @Override
    @NonNull
    public HttpClient stop() {
        httpClient.stop();
        return this;
    }

    public static RxStreamingHttpClient create(@Nullable URL url, @NonNull HttpClientConfiguration configuration) {
        return new ExtendedBridgedRxHttpClient(StreamingHttpClient.create(url, configuration));
    }
}
