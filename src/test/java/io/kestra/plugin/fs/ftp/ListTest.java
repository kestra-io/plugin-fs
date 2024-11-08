package io.kestra.plugin.fs.ftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 6; i++) {
            lastFile = IdUtils.create();
            ftpUtils.upload("upload" + dir + "/" + lastFile + ".yaml");
            ftpUtils.upload("upload" + dir + "/subfolder/" + lastFile + ".yaml");
        }
        ftpUtils.upload("upload" + dir + "/file with space.yaml");

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.of("/upload" + dir))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(Property.of("guest"))
            .password(Property.of("guest"));

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(7));

        task = builder
            .regExp(Property.of(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)"))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.of("/upload" + dir))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(Property.of("guest"))
            .password(Property.of("guest"))
            .recursive(Property.of(true)).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(13));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.of("/" + dir))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(Property.of("guest"))
            .password(Property.of("guest"))
            .recursive(Property.of(true)).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(0));
    }
}
