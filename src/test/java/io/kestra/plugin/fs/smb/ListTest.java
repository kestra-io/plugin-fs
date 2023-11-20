package io.kestra.plugin.fs.smb;

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
    private SmbUtils smbUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 6; i++) {
            lastFile = IdUtils.create();
            smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/" + lastFile + ".yaml");
            smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/subfolder/" + lastFile + ".yaml");
        }
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/file with space.yaml");

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(SmbUtils.SHARE_NAME + dir)
            .host("localhost")
            .username("alice")
            .password("alipass");

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(7));

        task = builder
            .regExp(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)")
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(1));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(SmbUtils.SHARE_NAME + dir)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .recursive(true).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(13));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(SmbUtils.SECOND_SHARE_NAME + dir)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .recursive(true)
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(0));
    }
}
