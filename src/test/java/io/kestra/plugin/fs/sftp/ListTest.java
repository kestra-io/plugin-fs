package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@KestraTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void all() throws Exception {
        java.util.List<LogEntry> logs = new ArrayList<>();
        var receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));
        String expectedEnabledRsaSha1Logs = "RSA/SHA1 is enabled, be advise that SHA1 is no longer considered secure by the general cryptographic community.";

        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 6; i++) {
            lastFile = IdUtils.create();
            sftpUtils.upload("upload" + dir + "/" + lastFile + ".yaml");
        }
        sftpUtils.upload("upload" + dir + "/file with space.yaml");

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload/" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .rootDir(Property.ofValue(false))
            .enableSshRsa1(Property.ofValue(true));

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(7));
        run.getFiles().forEach(file -> assertThat(file.getSize(), is(greaterThan(0L))));

        task = builder
            .regExp(Property.ofValue(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)"))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains(expectedEnabledRsaSha1Logs));
        receive.blockLast();
        assertThat(logs.stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains(expectedEnabledRsaSha1Logs)), is(true));
    }
}
