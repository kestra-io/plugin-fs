package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.commons.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void run_DeleteAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    private static Stream<Arguments> moveAfterDownloadsDelimiterAndFileExtension() {
        return Stream.of(
          Arguments.of("", ""),
          Arguments.of("", ".txt"),
          Arguments.of("/", ""),
          Arguments.of("/", ".txt")
        );
    }

    @ParameterizedTest
    @MethodSource("moveAfterDownloadsDelimiterAndFileExtension")
    void run_MoveAfterDownloads(String dirDelimiter, String fileExtension) throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + fileExtension);
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + fileExtension);

        String archiveShareDirectory = SmbUtils.SECOND_SHARE_NAME + "/" + rootFolder;

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .moveDirectory(Property.of(archiveShareDirectory + dirDelimiter))
            .action(Property.of(Downloads.Action.MOVE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        if (StringUtils.isNotBlank(fileExtension)) {
            assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(fileExtension));
        }
        assertThat(run.getOutputFiles().size(), is(2));

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().isEmpty(), is(true));

        task = task.toBuilder()
            .from(Property.of(archiveShareDirectory))
            .build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().size(), is(2));
        if (StringUtils.isNotBlank(fileExtension)) {
            assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(fileExtension));
        }
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
        assertThat(smbUtils.list(archiveShareDirectory).getFiles().size(), is(2));
    }

    @Test
    void run_NoneAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().size(), is(2));
    }

    @Test
    void run_shouldDownloadFileWithNameContainingDotsAndSpaces() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;

        final String fileName1 = IdUtils.create() + "file 1 name with spaces .and some dots.txt";
        final String fileName2 = IdUtils.create() + "file 2 name with spaces .and some dots.txt";

        smbUtils.upload(toUploadDir + "/" + fileName1);
        smbUtils.upload(toUploadDir + "/" + fileName2);

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        List<File> files = smbUtils.list(toUploadDir).getFiles();
        assertThat(files.size(), is(2));
        assertThat(files.stream().map(File::getName).toList().toArray(), arrayContainingInAnyOrder(fileName1, fileName2));
    }
}
