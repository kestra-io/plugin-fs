package io.kestra.plugin.fs.http;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.multipart.StreamingFileUpload;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.reactivex.Single;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import org.reactivestreams.Publisher;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@MicronautTest
class RequestTest {
    public static final String FILE = "http://www.mocky.io/v2/5ed0ce483500009300ff9f55";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        final String url = "http://www.mocky.io/v2/5ed0ce483500009300ff9f55";

        Request task = Request.builder()
            .id(RequestTest.class.getSimpleName())
            .type(RequestTest.class.getName())
            .uri(url)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        Request.Output output = task.run(runContext);

        assertThat(output.getUri(), is(URI.create(url)));
        assertThat(output.getBody(), is("{ \"hello\": \"world\" }"));
        assertThat(output.getCode(), is(200));
    }

    @Test
    void head() throws Exception {
        final String url = "https://proof.ovh.net/files/100Mb.dat";

        Request task = Request.builder()
            .id(RequestTest.class.getSimpleName())
            .type(RequestTest.class.getName())
            .uri(url)
            .method(HttpMethod.HEAD)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        Request.Output output = task.run(runContext);

        assertThat(output.getUri(), is(URI.create(url)));
        assertThat(output.getHeaders().get("Content-Length").get(0), is("104857600"));
    }

    @Test
    void failed() throws Exception {
        final String url = "http://www.mocky.io/v2/5ed0d31c3500009300ff9f94";

        Request task = Request.builder()
            .id(RequestTest.class.getSimpleName())
            .type(RequestTest.class.getName())
            .uri(url)
            .allowFailed(true)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        Request.Output output = task.run(runContext);

        assertThat(output.getUri(), is(URI.create(url)));
        assertThat(output.getBody(), is("{ \"hello\": \"world\" }"));
        assertThat(output.getCode(), is(417));
    }

    @Test
    void selfSigned() throws Exception {
        final String url = "https://self-signed.badssl.com/";

        Request task = Request.builder()
            .id(RequestTest.class.getSimpleName())
            .type(RequestTest.class.getName())
            .uri(url)
            .allowFailed(true)
            .sslOptions(AbstractHttp.SslOptions.builder().insecureTrustAllCertificates(true).build())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

        Request.Output output = task.run(runContext);

        assertThat(output.getUri(), is(URI.create(url)));
        assertThat(output.getBody(), containsString("self-signed.<br>badssl.com"));
        assertThat(output.getCode(), is(200));
    }

    @Test
    void form() throws Exception {
        try (
            ApplicationContext applicationContext = ApplicationContext.run();
            EmbeddedServer server = applicationContext.getBean(EmbeddedServer.class).start();

        ) {
            Request task = Request.builder()
                .id(RequestTest.class.getSimpleName())
                .type(RequestTest.class.getName())
                .method(HttpMethod.POST)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .uri(server.getURL().toString() + "/post/simple")
                .headers(ImmutableMap.of(
                    "test", "{{ inputs.test }}"
                ))
                .formData(ImmutableMap.of("hello", "world"))
                .build();


            RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of(
                "test", "value"
            ));

            Request.Output output = task.run(runContext);

            assertThat(output.getBody(), is("world > value"));
            assertThat(output.getCode(), is(200));
        }
    }

    @Test
    void multipart() throws Exception {
        File file = new File(Objects.requireNonNull(RequestTest.class.getClassLoader().getResource("application.yml")).toURI());

        URI fileStorage = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(file)
        );

        try (
            ApplicationContext applicationContext = ApplicationContext.run();
            EmbeddedServer server = applicationContext.getBean(EmbeddedServer.class).start();

        ) {
            Request task = Request.builder()
                .id(RequestTest.class.getSimpleName())
                .type(RequestTest.class.getName())
                .method(HttpMethod.POST)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .uri(server.getURL().toString() + "/post/multipart")
                .formData(ImmutableMap.of("hello", "world", "file", fileStorage.toString()))
                .build();

            RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

            Request.Output output = task.run(runContext);

            assertThat(output.getBody(), is("world > " + IOUtils.toString(new FileInputStream(file), Charsets.UTF_8)));
            assertThat(output.getCode(), is(200));
        }
    }

    @Controller(value = "/post", consumes = MediaType.APPLICATION_FORM_URLENCODED)
    static class MockController {
        @Post("/simple")
        HttpResponse<String> simple(HttpRequest<?> request, String hello) {
            return HttpResponse.ok(hello + " > " + request.getHeaders().get("test"));
        }

        @Post(uri = "/multipart", consumes = MediaType.MULTIPART_FORM_DATA)
        Single<String> multipart(HttpRequest<?> request, String hello, StreamingFileUpload file) throws IOException {
            File tempFile = File.createTempFile(file.getFilename(), "temp");

            Publisher<Boolean> uploadPublisher = file.transferTo(tempFile);

            return Single.fromPublisher(uploadPublisher)
                .map(success -> {
                    try (FileInputStream fileInputStream = new FileInputStream(tempFile)) {
                        return hello + " > " + IOUtils.toString(fileInputStream, StandardCharsets.UTF_8);
                    }
                });
        }
    }
}
