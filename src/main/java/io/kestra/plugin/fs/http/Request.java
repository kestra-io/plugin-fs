package io.kestra.plugin.fs.http;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.rxjava2.http.client.RxHttpClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Request an http server",
    description = "This task connects to http server, request the provided url and store the response as output"
)
@Plugin(
    examples = {
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
        ),
        @Example(
            title = "Post a multipart request to a webserver",
            code = {
                "uri: \"https://server.com/upload\"",
                "headers: ",
                "  user-agent: \"kestra-io\"",
                "method: \"POST\"",
                "contentType: \"multipart/form-data\"",
                "formData:",
                "  user: \"{{ inputs.file }}\"",
            }
        )
    }
)
public class Request extends AbstractHttp implements RunnableTask<Request.Output> {
    @Builder.Default
    @Schema(
        title = "If true, allow failed response code (response code >=400)"
    )
    protected boolean allowFailed = false;

    @SuppressWarnings("unchecked")
    public Request.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (
            RxHttpClient client = this.client(runContext, this.method);
        ) {
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
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The url of the current request"
        )
        private final URI uri;

        @Schema(
            title = "The status code of the response"
        )
        private final Integer code;

        @Schema(
            title = "The headers of the response"
        )
        @PluginProperty(additionalProperties = List.class)
        private final Map<String, List<String>> headers;

        @Schema(
            title = "The body of the response"
        )
        private final String body;
    }
}
