package org.kestra.task.fs;

import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

import java.net.URI;

/**
 * Input or Output can be nested as you need
 */
@Builder
@Getter
public class Output implements org.kestra.core.models.tasks.Output {
    @OutputProperty(
        description = "Short description for this output",
        body = "Full description of this output"
    )
    private URI from;


    private URI to;
}