package org.kestra.task.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class SftpUtils {
    @Inject
    private StorageInterface storageInterface;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    URI uploadToStorage() throws Exception {
        File applicationFile = new File(Objects.requireNonNull(SftpTest.class.getClassLoader()
            .getResource("application.yml"))
            .toURI()
        );

        return storageInterface.put(
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );
    }

    SftpOutput upload(String to) throws Exception {
        return this.upload(uploadToStorage(), to);
    }

    SftpOutput upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to(to)
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .rootDir(true)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
