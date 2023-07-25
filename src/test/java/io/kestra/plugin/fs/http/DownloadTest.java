package io.kestra.plugin.fs.http;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class DownloadTest {
    public static final String FILE = "https://proof.ovh.net/files/1Mb.dat";
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Inject
    private ApplicationContext applicationContext;

    @Test
    void run() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(DownloadTest.class.getName())
            .uri(FILE)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        Download.Output output = task.run(runContext);

        assertThat(
            IOUtils.toString(this.storageInterface.get(output.getUri()), StandardCharsets.UTF_8),
            is(IOUtils.toString(new URL(FILE).openStream(), StandardCharsets.UTF_8))
        );
    }

    @Test
    void noResponse() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(DownloadTest.class.getName())
            .uri("https://run.mocky.io/v3/bd4e25ed-2de1-44c9-b613-9612c965684b")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        HttpClientResponseException exception = assertThrows(
            HttpClientResponseException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("Service Unavailable"));
    }

    @Test
    void allowNoResponse() throws IOException {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .failOnEmptyResponse(false)
            .type(DownloadTest.class.getName())
            .uri("https://run.mocky.io/v3/513a88cf-65fc-4819-9fbf-3a3216a998c4")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());
        Download.Output output = assertDoesNotThrow(() -> task.run(runContext));

        assertThat(output.getLength(), is(0L));
        assertThat(IOUtils.toString(this.storageInterface.get(output.getUri()), StandardCharsets.UTF_8), is(""));
    }

    @Test
    void error() throws Exception {
        EmbeddedServer embeddedServer = applicationContext.getBean(EmbeddedServer.class);
        embeddedServer.start();

        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(DownloadTest.class.getName())
            .uri(embeddedServer.getURI() + "/500")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        HttpClientResponseException exception = assertThrows(
            HttpClientResponseException.class,
            () -> task.run(runContext)
        );

        assertThat(exception.getMessage(), is("Internal Server Error"));
    }

    @Controller()
    public static class SlackWebController {
        @Get("500")
        public HttpResponse<String> error() {
            return HttpResponse.serverError();
        }
    }
}
