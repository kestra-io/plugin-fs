package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractFileTriggerTest {
    @Inject
    private FtpUtils ftpUtils;

    @Override
    protected String triggeringFlowId() {
        return "ftp-listen";
    }

    @Override
    protected AbstractUtils utils() {
        return ftpUtils;
    }

    protected AbstractTrigger createTrigger(String from, Downloads.Action action, String moveDirectory) {
        return Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue(from))
            .action(Property.ofValue(action))
            .moveDirectory(Property.ofValue(moveDirectory))
            .passiveMode(Property.ofValue(true))
            .build();
    }

    @Test
    void sortAppliesBeforeMaxFilesTruncation() throws Exception {
        String dir = "/upload/trigger-sort-" + IdUtils.create();
        ftpUtils.upload(dir + "/b.txt");
        ftpUtils.upload(dir + "/a.txt");
        ftpUtils.upload(dir + "/c.txt");

        var trigger = Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(Downloads.Action.NONE))
            .sort(Property.ofValue(List.Sort.NAME_DESC))
            .maxFiles(Property.ofValue(2))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = ((PollingTriggerInterface) trigger).evaluate(context.getKey(), context.getValue().context());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> files = (java.util.List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));
        assertThat(files.stream().map(file -> (String) file.get("name")).toList(), contains("c.txt", "b.txt"));

        ftpUtils.delete(dir + "/a.txt");
        ftpUtils.delete(dir + "/b.txt");
        ftpUtils.delete(dir + "/c.txt");
    }
}
