package org.kestra.task.fs.http;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.netty.DefaultHttpClient;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.Plugin;
import org.kestra.core.models.executions.metrics.Counter;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download file from http server",
    description = "This task connects to http server and copy file to kestra file storage"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "headers: ",
                "  user-agent: \"kestra-io\"",
                "uri: \"https://server.com/file\""
            }
        )
    }
)
public class Download extends AbstractHttp implements RunnableTask<Download.Output> {
    public Download.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        URI from = new URI(runContext.render(this.uri));


        // temp file where download will be copied
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        // output
        Output.OutputBuilder builder = Output.builder();

        // do it
        try (
            DefaultHttpClient client = this.client(runContext);
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
        ) {
            @SuppressWarnings("unchecked")
            HttpRequest<String> request = this.request(runContext);

            Long size = client
                .exchangeStream(request)
                .map(response -> {
                    if (builder.code == null) {
                        builder
                            .code(response.code())
                            .headers(response.getHeaders().asMap());
                    }

                    if (response.getBody().isPresent()) {
                        byte[] bytes = response.getBody().get().toByteArray();
                        output.write(bytes);

                        return (long) bytes.length;
                    } else {
                        return 0L;
                    }
                })
                .reduce(Long::sum)
                .blockingGet();

            if (builder.headers.containsKey("Content-Length")) {
                long length = Long.parseLong(builder.headers.get("Content-Length").get(0));
                if (length != size) {
                    throw new IllegalStateException("Invalid size, got " + size + ", expexted " + length);
                }
            }

            output.flush();

            runContext.metric(Counter.of("response.length", size, this.tags(request, null)));
            builder.uri(runContext.putTempFile(tempFile));

            logger.debug("File '{}' download to '{}'", from, builder.uri);

            return builder.build();
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @Schema(
            title = "The url of the downloaded file on kestra storage"
        )
        private final URI uri;

        @Schema(
            title = "The status code of the response"
        )
        private final Integer code;

        @Schema(
            title = "The headers of the response"
        )
        private final Map<String, List<String>> headers;
    }
}
