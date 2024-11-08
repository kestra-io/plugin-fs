package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Test
    void run() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(from);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(Property.of("foo"))
            .password(Property.of("pass"))
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUri().getPath(), containsString(from));
        assertThat(run.isDeleted(), is(true));
    }
}
