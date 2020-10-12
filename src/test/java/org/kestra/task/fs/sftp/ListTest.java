package org.kestra.task.fs.sftp;

import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.utils.IdUtils;
import org.kestra.core.utils.TestsUtils;

import javax.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 7; i++) {
            lastFile = IdUtils.create();
            sftpUtils.upload("upload" + dir + "/" + lastFile + ".yaml");
        }

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .from("upload/" + dir)
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass");

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(7));
        run.getFiles().forEach(file -> assertThat(file.getSize(), is(greaterThan(0L))));

        task = builder
            .regExp(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)")
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(1));
    }
}
