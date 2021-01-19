package org.kestra.task.fs.http;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadTest {
    public static final String FILE = "https://file-examples.com/wp-content/uploads/2017/02/file_example_CSV_5000.csv";
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

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
}
