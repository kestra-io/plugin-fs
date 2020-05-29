package org.kestra.task.fs.http;

import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.DefaultHttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Request an http server",
    body = "This task connects to http server, request the provided url and store the response as output"
)
@Example(
    title = "Post request to a webserver",
    code = {
        "uri: \"https://server.com/login\"",
        "headers: ",
        "  user-agent: \"kestra-io\"",
        "method: \"POST\"",
        "formData:",
        "  user: \"user\"",
        "  password: \"pass\""
    }
)
public class Request extends AbstractHttp implements RunnableTask<Request.Output> {
    @Builder.Default
    @InputProperty(
        description = "If true, allow failed response code (response code >=400)"
    )
    protected boolean allowFailed = false;

    public Request.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger(getClass());

        try (
            DefaultHttpClient client = this.client(runContext);
        ) {
            @SuppressWarnings("unchecked")
            HttpRequest<String> request = this.request(runContext);
            HttpResponse<String> response;

            try {
                response = client
                    .toBlocking()
                    .exchange(request, Argument.STRING, Argument.STRING);
            } catch (HttpClientResponseException e) {
                if (!allowFailed) {
                    throw e;
                }

                //noinspection unchecked
                response = (HttpResponse<String>) e.getResponse();
            }

            logger.debug("Request '{}' with response code '{}'", request.getUri(), response.getStatus().getCode());

            return this.output(runContext, request, response);
        }
    }

    public Request.Output output(RunContext runContext, HttpRequest<String> request, HttpResponse<String> response) {
        response
            .getHeaders()
            .contentLength()
            .ifPresent(value -> {
                runContext.metric(Counter.of(
                    "response.length", value,
                    this.tags(request, response)
                ));
            });

        return Output.builder()
            .code(response.getStatus().getCode())
            .headers(response.getHeaders().asMap())
            .uri(request.getUri())
            .body(response.body())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The url of the downloaded file on kestra storage"
        )
        private final URI uri;

        @OutputProperty(
            description = "The status code of the response"
        )
        private final Integer code;

        @OutputProperty(
            description = "The headers of the response"
        )
        private final Map<String, List<String>> headers;

        @OutputProperty(
            description = "The body of the response"
        )
        private final String body;
    }
}
