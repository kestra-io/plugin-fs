package org.kestra.task.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.test.annotation.MicronautTest;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.kestra.core.runners.RunContext;
import org.kestra.core.runners.RunContextFactory;
import org.kestra.core.storages.StorageInterface;
import org.kestra.core.utils.TestsUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
