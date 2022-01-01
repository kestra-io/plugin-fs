package io.kestra.plugin.fs.ftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 7; i++) {
            lastFile = IdUtils.create();
            ftpUtils.upload("upload" + dir + "/" + lastFile + ".yaml");
        }

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from("/upload" + dir)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest");

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(7));

        task = builder
            .regExp(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)")
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(1));
    }
}
