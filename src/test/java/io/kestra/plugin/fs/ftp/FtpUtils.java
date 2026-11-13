package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

@Singleton
public class FtpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;
    public static final Property<String> USERNAME = Property.ofValue("guest");
    public static final Property<String> PASSWORD = Property.ofValue("O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+");

    /**
     * Writes a file with a raw commons-net client, bypassing Commons VFS and its file-name
     * encoding, so the file on the server has exactly the given name.
     */
    public void putRaw(String dir, String fileName, String content) throws Exception {
        withClient(client -> {
            var current = "";
            for (var segment : dir.split("/")) {
                if (segment.isEmpty()) {
                    continue;
                }
                current += "/" + segment;
                client.makeDirectory(current); // fails harmlessly when it already exists
            }
            if (!client.storeFile(dir + "/" + fileName, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))) {
                throw new IOException("Unable to store '" + dir + "/" + fileName + "': " + client.getReplyString());
            }
            return null;
        });
    }

    /**
     * Lists the file names of a directory with a raw commons-net client, bypassing Commons VFS;
     * returns an empty list when the directory doesn't exist.
     */
    public java.util.List<String> listRaw(String dir) throws Exception {
        return withClient(client -> {
            if (!client.changeWorkingDirectory(dir)) {
                return java.util.List.of();
            }
            return Arrays.stream(client.listFiles())
                .filter(FTPFile::isFile)
                .map(FTPFile::getName)
                .toList();
        });
    }

    private <T> T withClient(FtpAction<T> action) throws Exception {
        var client = new FTPClient();
        client.connect("localhost", 6621);
        try {
            if (!client.login("guest", "O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+")) {
                throw new IOException("FTP login failed: " + client.getReplyString());
            }
            client.enterLocalPassiveMode();
            client.setFileType(FTP.BINARY_FILE_TYPE);
            return action.apply(client);
        } finally {
            client.disconnect();
        }
    }

    @FunctionalInterface
    private interface FtpAction<T> {
        T apply(FTPClient client) throws IOException;
    }

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(FtpUtils.class.getSimpleName())
            .type(FtpUtils.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Override
    public List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.ftp.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(Property.ofValue(dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build()
            .run(this.runContextFactory.of());
    }

    @Override
    public Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(FtpUtils.class.getSimpleName())
            .type(FtpUtils.class.getName())
            .uri(Property.ofValue(file))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
