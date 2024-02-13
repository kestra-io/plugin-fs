package io.kestra.plugin.fs.http;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.EncryptedString;
import io.kestra.core.runners.RunContext;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Make an HTTP request to a server",
    description = """
                  This task connects to a HTTP server, sends a request, and stores the response as output.
                  By default, the maximum length of the response is limited to 10MB but it can be increased to at most 2GB by using the `options.maxContentLength` property.
                  Note that the response is added as output to the task, to download large content it is advised to use the Download task instead."""
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
        ),
        @Example(
            title = "Post a multipart request to a webserver while renaming the file sent",
            code = {
                "uri: \"https://server.com/upload\"",
                "headers: ",
                "  user-agent: \"kestra-io\"",
                "method: \"POST\"",
                "contentType: \"multipart/form-data\"",
                "formData:",
                "  user:",
                "    name: \"my-file.txt\"",
                "    content: \"{{ inputs.file }}\"",
            }
        )
    }
)
public class Request extends AbstractHttp implements RunnableTask<Request.Output> {
    @Builder.Default
    @Schema(
        title = "If true, allow failed response code (response code >=400)"
    )
    private boolean allowFailed = false;

    @Builder.Default
    @Schema(
        title = "If true, the HTTP response body will be automatically encrypted and decrypted in the outputs if encryption is configured",
        description = "When true, the `encryptedBody` output will be filled, otherwise the `body` output will be filled"
    )
    private boolean encryptBody = false;

    @SuppressWarnings("unchecked")
    public Request.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (HttpClient client = this.client(runContext, this.method)) {
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

    public Request.Output output(RunContext runContext, HttpRequest<String> request, HttpResponse<String> response) throws GeneralSecurityException {
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
            .body(encryptBody ? null : response.body())
            .encryptedBody(encryptBody ? EncryptedString.from(response.body(), runContext) : null)
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
            title = "The body of the response",
            description = "Only set of `encryptBody` is set to false, otherwise the `encryptedBody` output will be set instead."
        )
        private Object body;

        @Schema(
            title = "The encrypted body of the response, ity will be automatically encrypted and decrypted in the outputs",
            description = "Only set of `encryptBody` is set to true, otherwise the `body` output will be set instead."
        )
        private EncryptedString encryptedBody;

        @Schema(
            title = "The form data to be send",
            description = "When sending a file, you can pass a map with a key 'name' for the filename and 'content' for the file content."
        )
        @PluginProperty(dynamic = true)
        protected Map<String, Object> formData;
    }
}
