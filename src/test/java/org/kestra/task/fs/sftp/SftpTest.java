package org.kestra.task.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.storages.StorageInterface;
import org.kestra.task.fs.Output;

import javax.inject.Inject;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class SftpTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        testSftp(true);
        testSftp(false);
    }

    void testSftp(Boolean keyAuth) throws Exception {
        File applicationFile = new File(Objects.requireNonNull(SftpTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI());
        URI source = storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );

        String sftpPath = "upload/testSftpSource";


        File file = new File(System.getProperty("user.home") + "/.ssh/id_rsa");
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String keyFileContent = new String(data, "UTF-8");

        RunContext runContextUpload = new RunContext(this.applicationContext, new HashMap<String, Object>());
        var ulbuild = SftpUpload.builder()
            .from(source.toString())
            .to(sftpPath)
            .host("localhost")
            .port("6622")
            .username("foo");

        if (keyAuth) {
            ulbuild.keyfile(keyFileContent).passPhrase("testPassPhrase");
        } else {
            ulbuild.password("pass");
        }
        SftpUpload taskUpload = ulbuild.build();
        Output runOutputUpload = taskUpload.run(runContextUpload);

        new HashMap<String, Object>();
        String localPathCheck = "/tmp/sftp_test_file_download";
        var dlbuild = SftpDownload.builder()
            .from(sftpPath)
            .host("localhost")
            .port("6622")
            .username("foo");

        if (keyAuth) {
            dlbuild.keyfile(keyFileContent).passPhrase("testPassPhrase");
        } else {
            ulbuild.password("pass");
        }
        SftpDownload taskDownload = dlbuild.build();

        RunContext runContextDownload = new RunContext(this.applicationContext, new HashMap<String, Object>());
        Output runOutputDownload = taskDownload.run(runContextDownload);
        URI to = runOutputDownload.getTo();

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(to.getPath())
        );

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContextDownload.uriToInputStream(to), outputStream);
        }

        //load transfered file
        FileInputStream fisCheck = new FileInputStream(tempFile);
        byte[] dataCheck = new byte[(int) tempFile.length()];
        fisCheck.read(dataCheck);
        fisCheck.close();
        //load source file
        FileInputStream fisCheckCompare = new FileInputStream(applicationFile);
        byte[] dataCheckCompare = new byte[(int) applicationFile.length()];
        fisCheckCompare.read(dataCheckCompare);
        fisCheckCompare.close();
        //compare content with go and back on sftp server
        assertThat(new String(dataCheck, "UTF-8"), is(new String(dataCheckCompare, "UTF-8")));
    }
}