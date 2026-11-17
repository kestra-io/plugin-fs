package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.Uploads.Output;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import io.kestra.core.exceptions.KestraRuntimeException;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class UploadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    private final String random = IdUtils.create();

    @Test
    void run_withMapShouldPreserveFilenames() throws Exception {
        URI uri1 = sftpUtils.uploadToStorage();
        URI uri2 = sftpUtils.uploadToStorage();
        Uploads uploads = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Map.of("report.csv", uri1.toString(), "data.json", uri2.toString()))
            .to(Property.ofValue("/upload/" + random))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Output uploadsRun = uploads.run(TestsUtils.mockRunContext(runContextFactory, uploads, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        List<String> filePaths = uploadsRun.getFiles().stream().map(URI::getPath).toList();
        assertThat(filePaths.stream().anyMatch(p -> p.endsWith("/report.csv")), is(true));
        assertThat(filePaths.stream().anyMatch(p -> p.endsWith("/data.json")), is(true));

        // Cleanup
        Downloads task = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.ofValue("/upload/" + random + "/"))
            .action(Property.ofValue(Downloads.Action.DELETE))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Test
    void run_withJsonMapStringShouldPreserveFilenames() throws Exception {
        URI uri1 = sftpUtils.uploadToStorage();
        URI uri2 = sftpUtils.uploadToStorage();
        // Simulates {{ outputs.download_files.outputFiles }} rendering to a JSON object string
        String jsonMapFrom = "{\"report.csv\":\"" + uri1 + "\",\"data.json\":\"" + uri2 + "\"}";
        Uploads uploads = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(jsonMapFrom)
            .to(Property.ofValue("/upload/" + random))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Output uploadsRun = uploads.run(TestsUtils.mockRunContext(runContextFactory, uploads, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        List<String> filePaths = uploadsRun.getFiles().stream().map(URI::getPath).toList();
        assertThat(filePaths.stream().anyMatch(p -> p.endsWith("/report.csv")), is(true));
        assertThat(filePaths.stream().anyMatch(p -> p.endsWith("/data.json")), is(true));

        // Cleanup
        Downloads task = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.ofValue("/upload/" + random + "/"))
            .action(Property.ofValue(Downloads.Action.DELETE))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Test
    void run_withOverwriteFalseShouldFailWhenFileExists() throws Exception {
        URI uri1 = sftpUtils.uploadToStorage();
        String dir = "/upload/" + IdUtils.create();

        Uploads first = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString()))
            .to(Property.ofValue(dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        first.run(TestsUtils.mockRunContext(runContextFactory, first, Map.of()));

        Uploads second = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString()))
            .to(Property.ofValue(dir))
            .overwrite(Property.ofValue(false))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> second.run(TestsUtils.mockRunContext(runContextFactory, second, Map.of()))
        );
        assertThat(ex.getMessage(), containsString("already exists"));

        cleanupDir(dir);
    }

    private void cleanupDir(String dir) throws Exception {
        Downloads cleanup = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(Downloads.Action.DELETE))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        cleanup.run(TestsUtils.mockRunContext(runContextFactory, cleanup, Map.of()));
    }

    @Test
    void run() throws Exception {
        URI uri1 = sftpUtils.uploadToStorage();
        URI uri2 = sftpUtils.uploadToStorage();
        Uploads uploads = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue("/upload/" + random))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Output uploadsRun = uploads.run(TestsUtils.mockRunContext(runContextFactory, uploads, Map.of()));

        Downloads task = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.ofValue("/upload/" + random + "/"))
            .action(Property.ofValue(Downloads.Action.DELETE))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output downloadsRun = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(2));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
    }

    @Test
    void run_withKeyfileShouldBuildKeyFileOnlyOnce() throws Exception {
        var key = Files.readString(Path.of("src/test/resources/ssh/id_ed25519"), StandardCharsets.UTF_8);
        var dir = "/upload/" + IdUtils.create();
        var uris = List.of(
            sftpUtils.uploadToStorage().toString(),
            sftpUtils.uploadToStorage().toString(),
            sftpUtils.uploadToStorage().toString()
        );
        var uploads = Uploads.builder()
            .id(IdUtils.create())
            .type(Uploads.class.getName())
            .from(uris)
            .to(Property.ofValue(dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .keyfile(Property.ofValue(key))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, uploads, Map.of());

        var output = uploads.run(runContext);

        assertThat(output.getFiles().size(), is(3));

        // VfsService.upload also creates one temp file per upload, so match on key content
        long keyFiles;
        try (var stream = Files.list(runContext.workingDir().path())) {
            keyFiles = stream
                .filter(Files::isRegularFile)
                .filter(f -> {
                    try {
                        return Files.readString(f).equals(key);
                    } catch (IOException e) {
                        return false;
                    }
                })
                .count();
        }
        assertThat(keyFiles, is(1L));

        var list = io.kestra.plugin.fs.sftp.List.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.sftp.List.class.getName())
            .from(Property.ofValue(dir + "/"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .keyfile(Property.ofValue(key))
            .build();
        var listed = list.run(TestsUtils.mockRunContext(runContextFactory, list, Map.of()));
        assertThat(listed.getFiles().size(), is(3));

        cleanupDir(dir);
    }

    @Test
    void shouldFailWhenDuplicateDestinationFilenames() {
        var uploads = Uploads.builder()
            .id(IdUtils.create())
            .type(Uploads.class.getName())
            .from(List.of("kestra:///step1/report.csv", "kestra:///step2/report.csv"))
            .to(Property.ofValue("/upload/" + random))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> uploads.run(TestsUtils.mockRunContext(runContextFactory, uploads, Map.of()))
        );

        assertThat(exception.getMessage(), containsString("Multiple files resolve to the same destination filename 'report.csv'"));
    }
}
