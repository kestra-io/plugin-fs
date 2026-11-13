package io.kestra.plugin.fs.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Delete;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

@Singleton
class SftpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;

    public static final Property<String> USERNAME = Property.ofValue("foo");
    public static final Property<String> PASSWORD = Property.ofValue("pass*+=");

    /**
     * Writes a file with a raw JSch channel, bypassing Commons VFS and its file-name encoding,
     * so the file on the server has exactly the given name.
     */
    public void putRaw(String dir, String fileName, String content) throws Exception {
        withChannel(channel -> {
            var current = "";
            for (var segment : dir.split("/")) {
                if (segment.isEmpty()) {
                    continue;
                }
                current += "/" + segment;
                try {
                    channel.stat(current);
                } catch (SftpException e) {
                    channel.mkdir(current);
                }
            }
            channel.put(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), dir + "/" + fileName);
            return null;
        });
    }

    /**
     * Lists the file names of a directory with a raw JSch channel, bypassing Commons VFS;
     * returns an empty list when the directory doesn't exist.
     */
    public java.util.List<String> listRaw(String dir) throws Exception {
        return withChannel(channel -> {
            var names = new ArrayList<String>();
            try {
                for (var entry : channel.ls(dir)) {
                    if (!entry.getAttrs().isDir()) {
                        names.add(entry.getFilename());
                    }
                }
            } catch (SftpException e) {
                if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                    throw e;
                }
            }
            return names;
        });
    }

    private <T> T withChannel(SftpAction<T> action) throws Exception {
        Session session = new JSch().getSession("foo", "localhost", 6622);
        try {
            session.setPassword("pass*+=");
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            var channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            try {
                return action.apply(channel);
            } finally {
                channel.disconnect();
            }
        } finally {
            session.disconnect();
        }
    }

    @FunctionalInterface
    private interface SftpAction<T> {
        T apply(ChannelSftp channel) throws SftpException, JSchException;
    }

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .rootDir(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Override
    public io.kestra.plugin.fs.vfs.List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.sftp.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(Property.ofValue(dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build()
            .run(this.runContextFactory.of());
    }

    @Override
    public Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.sftp.Delete.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .uri(Property.ofValue(file))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .rootDir(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
