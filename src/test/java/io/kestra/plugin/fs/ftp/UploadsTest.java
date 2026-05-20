package io.kestra.plugin.fs.ftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class UploadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Inject
    private StorageInterface storageInterface;

    private final String random = IdUtils.create();

    private URI uploadWithSuffix(String suffix) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId() + suffix),
            new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void run() throws Exception {
        URI uri1 = ftpUtils.uploadToStorage();
        URI uri2 = ftpUtils.uploadToStorage();

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
                .type(UploadsTest.class.getName())
                .from(List.of(uri1.toString(), uri2.toString()))
                .to(Property.ofValue("/upload/" + random + "/"))
                .host(Property.ofValue("localhost"))
                .port(Property.ofValue("6621"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        URI uri3 = ftpUtils.uploadToStorage();
        URI uri4 = ftpUtils.uploadToStorage();
        uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
                .type(UploadsTest.class.getName())
                .from("{{ inputs.uris }}")
                .to(Property.ofValue("/upload/" + random + "/"))
                .host(Property.ofValue("localhost"))
                .port(Property.ofValue("6621"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();
        Uploads.Output uploadsRunTemplate = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask,
                Map.of("uris", "[\""+uri3.toString()+"\",\""+uri4.toString()+"\"]"))
        );

        Downloads downloadsTask = Downloads.builder()
                .id(UploadsTest.class.getSimpleName())
                .type(UploadsTest.class.getName())
                .from(Property.ofValue("/upload/" + random + "/"))
                .action(Property.ofValue(Downloads.Action.DELETE))
                .host(Property.ofValue("localhost"))
                .port(Property.ofValue("6621"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();

        Downloads.Output downloadsRun = downloadsTask.run(TestsUtils.mockRunContext(runContextFactory, downloadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(uploadsRunTemplate.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(4));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
        assertThat(uploadsRunTemplate.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
    }

    @Test
    void shouldFilterFilesByExtension() throws Exception {
        URI sqlUri = uploadWithSuffix(".sql");
        URI yamlUri = uploadWithSuffix(".yaml");
        URI txtUri = uploadWithSuffix(".txt");

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(sqlUri.toString(), yamlUri.toString(), txtUri.toString()))
            .to(Property.ofValue("/upload/" + random + "/sql/"))
            .regExp(Property.ofValue(".*\\.sql$"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(1));
        assertThat(uploadsRun.getFiles().getFirst().getPath(), Matchers.endsWith(".sql"));
    }

    @Test
    void shouldMatchMultipleExtensions() throws Exception {
        URI yml = uploadWithSuffix(".yml");
        URI yaml = uploadWithSuffix(".yaml");
        URI json = uploadWithSuffix(".json");

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(yml.toString(), yaml.toString(), json.toString()))
            .to(Property.ofValue("/upload/" + random + "/yaml/"))
            .regExp(Property.ofValue(".*\\.ya?ml$"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(
            uploadsRun.getFiles().stream().map(URI::getPath).toList(),
            Matchers.everyItem(Matchers.anyOf(Matchers.endsWith(".yml"), Matchers.endsWith(".yaml")))
        );
    }

    @Test
    void shouldUploadNothingWhenRegExpMatchesNoFile() throws Exception {
        URI uri1 = uploadWithSuffix(".sql");
        URI uri2 = uploadWithSuffix(".yaml");

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue("/upload/" + random + "/none/"))
            .regExp(Property.ofValue(".*\\.csv$"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(0));
    }

    @Test
    void run_maxFilesShouldLimit() throws Exception {
        URI uri1 = ftpUtils.uploadToStorage();
        URI uri2 = ftpUtils.uploadToStorage();

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue("/upload/" + random + "/max-files/"))
            .maxFiles(Property.ofValue(1))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(1));
    }
}
