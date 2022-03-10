package io.kestra.plugin.fs.http;

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.StreamingHttpClient;
import io.micronaut.rxjava2.http.client.RxHttpClient;
import io.micronaut.rxjava2.http.client.RxStreamingHttpClient;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import java.net.URL;
import java.util.Map;

/**
 * Hard copy since plugin can override package with compileOnly
 *
 * @see <a href="https://github.com/micronaut-projects/micronaut-rxjava2/pull/51">Waiting for release</a>
 */
@SuppressWarnings("rawtypes")
public class ExtendedBridgedRxHttpClient implements RxStreamingHttpClient, RxHttpClient {
    private final StreamingHttpClient httpClient;

    ExtendedBridgedRxHttpClient(StreamingHttpClient httpClient) {
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

    @Override
    public <I> Flowable<ByteBuffer<?>> dataStream(@NonNull HttpRequest<I> request) {
        return Flowable.fromPublisher(httpClient.dataStream(request));
    }

    @Override
    public <I> Publisher<ByteBuffer<?>> dataStream(@NonNull HttpRequest<I> request, @NonNull Argument<?> errorType) {
        return Flowable.fromPublisher(httpClient.dataStream(request, errorType));
    }

    @Override
    public <I> Flowable<HttpResponse<ByteBuffer<?>>> exchangeStream(@NonNull HttpRequest<I> request) {
        return Flowable.fromPublisher(httpClient.exchangeStream(request));
    }

    @Override
    public <I> Publisher<HttpResponse<ByteBuffer<?>>> exchangeStream(@NonNull HttpRequest<I> request, @NonNull Argument<?> errorType) {
        return Flowable.fromPublisher(httpClient.exchangeStream(request, errorType));
    }

    @Override
    public <I> Publisher<Map<String, Object>> jsonStream(@NonNull HttpRequest<I> request) {
        return Flowable.fromPublisher(httpClient.jsonStream(request));
    }

    @Override
    public <I, O> Flowable<O> jsonStream(@NonNull HttpRequest<I> request, @NonNull Argument<O> type) {
        return Flowable.fromPublisher(httpClient.jsonStream(request, type));
    }

    @Override
    public <I, O> Publisher<O> jsonStream(@NonNull HttpRequest<I> request, @NonNull Argument<O> type, @NonNull Argument<?> errorType) {
        return Flowable.fromPublisher(httpClient.jsonStream(request, type, errorType));
    }

    @Override
    public <I, O> Flowable<O> jsonStream(@NonNull HttpRequest<I> request, @NonNull Class<O> type) {
        return Flowable.fromPublisher(httpClient.jsonStream(request, type));
    }

    @Override
    public BlockingHttpClient toBlocking() {
        return httpClient.toBlocking();
    }

    @Override
    public <I, O, E> Flowable<HttpResponse<O>> exchange(@NonNull HttpRequest<I> request, @NonNull Argument<O> bodyType, @NonNull Argument<E> errorType) {
        return Flowable.fromPublisher(httpClient.exchange(request, bodyType, errorType));
    }

    @Override
    public <I, O> Flowable<HttpResponse<O>> exchange(@NonNull HttpRequest<I> request, @NonNull Argument<O> bodyType) {
        return Flowable.fromPublisher(httpClient.exchange(request, bodyType));
    }

    @Override
    public <I, O, E> Flowable<O> retrieve(@NonNull HttpRequest<I> request, @NonNull Argument<O> bodyType, @NonNull Argument<E> errorType) {
        return Flowable.fromPublisher(httpClient.retrieve(request, bodyType));
    }

    @Override
    public <I> Flowable<HttpResponse<ByteBuffer>> exchange(@NonNull HttpRequest<I> request) {
        return Flowable.fromPublisher(httpClient.exchange(request));
    }

    @Override
    public Flowable<HttpResponse<ByteBuffer>> exchange(@NonNull String uri) {
        return Flowable.fromPublisher(httpClient.exchange(uri));
    }

    @Override
    public <O> Flowable<HttpResponse<O>> exchange(@NonNull String uri, @NonNull Class<O> bodyType) {
        return Flowable.fromPublisher(httpClient.exchange(uri, bodyType));
    }

    @Override
    public <I, O> Flowable<HttpResponse<O>> exchange(@NonNull HttpRequest<I> request, @NonNull Class<O> bodyType) {
        return Flowable.fromPublisher(httpClient.exchange(request, bodyType));
    }

    @Override
    public <I, O> Flowable<O> retrieve(@NonNull HttpRequest<I> request, @NonNull Argument<O> bodyType) {
        return Flowable.fromPublisher(httpClient.retrieve(request, bodyType));
    }

    @Override
    public <I, O> Flowable<O> retrieve(@NonNull HttpRequest<I> request, @NonNull Class<O> bodyType) {
        return Flowable.fromPublisher(httpClient.retrieve(request, bodyType));
    }

    @Override
    public <I> Flowable<String> retrieve(@NonNull HttpRequest<I> request) {
        return Flowable.fromPublisher(httpClient.retrieve(request));
    }

    @Override
    public Flowable<String> retrieve(@NonNull String uri) {
        return Flowable.fromPublisher(httpClient.retrieve(uri));
    }

    @Override
    public boolean isRunning() {
        return httpClient.isRunning();
    }

}
