package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.plugin.fs.vfs.Download.Output;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class SftpTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void password() throws Exception {
        testSftp(false);
    }

    @Test
    void authKey() throws Exception {
        String keyFileContent = readKeyFile();
        RunContext runContext = runContextFactory.of();

        Upload uploadTask = Upload.builder()
            .from(Property.ofValue("dummy"))
            .to(Property.ofValue("upload"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .keyfile(Property.ofValue(keyFileContent))
            .build();

        FileSystemOptions options = SftpService.fsOptions(runContext, uploadTask);

        assertThat(SftpFileSystemConfigBuilder.getInstance().getIdentityProvider(options), notNullValue());
        assertThat(SftpFileSystemConfigBuilder.getInstance().getPreferredAuthentications(options), is("publickey"));
    }

    void testSftp(Boolean keyAuth) throws Exception {
        File applicationFile = new File(Objects.requireNonNull(SftpTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI()
        );
        URI source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );
        RunContext runContext = runContextFactory.of();

        String sftpPath = "upload/" + UUID.randomUUID();

        String keyFileContent = readKeyFile();

        // Upload task
        var uploadTask = Upload.builder()
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue(sftpPath))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME);

        if (keyAuth) {
            uploadTask = uploadTask
                .keyfile(Property.ofValue(keyFileContent));
        } else {
            uploadTask.password(PASSWORD);
        }

        Upload taskUpload = uploadTask.build();
        taskUpload.run(runContext);

        // Download task
        var downloadTask = Download.builder()
            .from(Property.ofValue(sftpPath))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME);

        if (keyAuth) {
            downloadTask = downloadTask
                .keyfile(Property.ofValue(keyFileContent));
        } else {
            downloadTask = downloadTask.password(PASSWORD);
        }

        Download taskDownload = downloadTask.build();
        Output runOutputDownload = taskDownload.run(runContext);
        URI to = runOutputDownload.getTo();

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(to.getPath())
        );

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.storage().getFile(to), outputStream);
        }

        // load transfered file
        byte[] dataCheck;
        try (FileInputStream fisCheck = new FileInputStream(tempFile)) {
            dataCheck = new byte[(int) tempFile.length()];
            fisCheck.read(dataCheck);
        }

        // load source file
        byte[] dataCheckCompare;
        try (FileInputStream fisCheckCompare = new FileInputStream(applicationFile)) {
            dataCheckCompare = new byte[(int) applicationFile.length()];
            fisCheckCompare.read(dataCheckCompare);
        }

        //compare content with go and back on sftp server
        assertThat(new String(dataCheck, StandardCharsets.UTF_8), is(new String(dataCheckCompare, StandardCharsets.UTF_8)));
    }

    private String readKeyFile() throws Exception {
        return Files.readString(Path.of("src/test/resources/ssh/id_ed25519"), StandardCharsets.UTF_8);
    }
}
