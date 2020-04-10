package org.kestra.task.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.storages.StorageInterface;

import javax.inject.Inject;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class SftpTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void password() throws Exception {
        testSftp(false);
    }

    @Test
    void authKey() throws Exception {
        testSftp(true);
    }

    void testSftp(Boolean keyAuth) throws Exception {
        File applicationFile = new File(Objects.requireNonNull(SftpTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI()
        );
        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );
        RunContext runContext = new RunContext(this.applicationContext, new HashMap<>());

        String sftpPath = "upload/" + UUID.randomUUID().toString();

        File file = new File(System.getProperty("user.dir") + "/id_rsa");
        byte[] data;
        try (FileInputStream fis = new FileInputStream(file)) {
            data = new byte[(int) file.length()];
            fis.read(data);
        }
        String keyFileContent = new String(data, StandardCharsets.UTF_8);

        // Upload task
        var uploadTask = Upload.builder()
            .from(source.toString())
            .to(sftpPath)
            .host("localhost")
            .port("6622")
            .username("foo");

        if (keyAuth) {
            uploadTask = uploadTask
                .keyfile(keyFileContent)
                .passphrase("testPassPhrase");
        } else {
            uploadTask.password("pass");
        }

        Upload taskUpload = uploadTask.build();
        taskUpload.run(runContext);

        // Download task
        var downloadTask = Download.builder()
            .from(sftpPath)
            .host("localhost")
            .port("6622")
            .username("foo");

        if (keyAuth) {
            downloadTask = downloadTask
                .keyfile(keyFileContent)
                .passphrase("testPassPhrase");
        } else {
            downloadTask = downloadTask.password("pass");
        }

        Download taskDownload = downloadTask.build();
        SftpOutput runOutputDownload = taskDownload.run(runContext);
        URI to = runOutputDownload.getTo();

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(to.getPath())
        );

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.uriToInputStream(to), outputStream);
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
}
