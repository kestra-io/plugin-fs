package org.kestra.task.fs.ftp;

import io.micronaut.context.ApplicationContext;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;

import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class FtpTest {
    @Inject
    private ApplicationContext applicationContext;

    @Test
    void run() throws Exception {
        String testFileContent = "testFileContent";
        String remotePath = "upload/testFtpSource";
        String localPath = "/tmp/testFtpFile";
        FileWriter myWriter = new FileWriter(localPath);
        myWriter.write(testFileContent);
        myWriter.close();

        var paramsUpload = new HashMap<String, Object>();

        paramsUpload.put("remotePath", remotePath);
        paramsUpload.put("localPath", localPath);
        paramsUpload.put("username", "foo");
        paramsUpload.put("password", "pass");
        paramsUpload.put("host", "localhost");
        paramsUpload.put("port", 6621);
        RunContext runContextUpload = new RunContext(this.applicationContext, paramsUpload);
        FtpUpload taskUpload = FtpUpload.builder()
            .remotePath("{{remotePath}}")
            .localPath("{{localPath}}")
            .username("{{username}}")
            .password("{{password}}")
            .host("{{host}}")
            .port("{{port}}")
            .build();

        String downloadLocalPath = "/tmp/testFtpResult";
        HashMap<String, Object> paramsDownload = (HashMap<String, Object>) paramsUpload.clone();
        paramsDownload.put("localPath", downloadLocalPath);
        RunContext runContextDownload = new RunContext(this.applicationContext, paramsDownload);
        FtpDownload taskDownload = FtpDownload.builder()
            .remotePath("{{remotePath}}")
            .localPath("{{localPath}}")
            .username("{{username}}")
            .password("{{password}}")
            .host("{{host}}")
            .port("{{port}}")
            .build();

        FtpUpload.Output runOutputUpload = taskUpload.run(runContextUpload);
        FtpDownload.Output runOutputDownload = taskDownload.run(runContextDownload);

        assertThat(runOutputUpload.getChild().getValue(), is(remotePath));
        assertThat(runOutputDownload.getChild().getValue(), is(downloadLocalPath));
        File file = new File(downloadLocalPath);
        FileInputStream fis = new FileInputStream(file);
        byte[] data = new byte[(int) file.length()];
        fis.read(data);
        fis.close();
        String downloadFileContent = new String(data, "UTF-8");
        assertThat(downloadFileContent, is(testFileContent));

    }
}