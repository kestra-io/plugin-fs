package org.kestra.task.fs.http;

import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpMethod;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.utils.TestsUtils;

import java.net.URI;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class RequestTest {
    public static final String FILE = "http://www.mocky.io/v2/5ed0ce483500009300ff9f55";
    @Inject
    private RunContextFactory runContextFactory;

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
                .formData(ImmutableMap.of("hello", "world"))
                .build();


            RunContext runContext = TestsUtils.mockRunContext(this.runContextFactory, task, ImmutableMap.of());

            Request.Output output = task.run(runContext);

            assertThat(output.getBody(), is("world"));
            assertThat(output.getCode(), is(200));
        }
    }

    @Controller(value = "/post", consumes = MediaType.APPLICATION_FORM_URLENCODED)
    static class MockController {
        @Post("/simple")
        HttpResponse<String> simple(String hello) {
            return HttpResponse.ok(hello);
        }
    }
}
