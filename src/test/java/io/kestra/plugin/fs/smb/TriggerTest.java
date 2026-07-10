package io.kestra.plugin.fs.smb;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

class TriggerTest extends AbstractFileTriggerTest {
    @Inject
    private SmbUtils smbUtils;

    @Override
    protected String triggeringFlowId() {
        return "smb-listen";
    }

    @Override
    protected AbstractUtils utils() {
        return smbUtils;
    }

    @Override
    protected AbstractTrigger createTrigger(String from, Downloads.Action action, String moveDirectory) {
        return io.kestra.plugin.fs.smb.Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(io.kestra.plugin.fs.smb.Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue(from))
            .action(Property.ofValue(action))
            .moveDirectory(Property.ofValue(moveDirectory))
            .build();
    }

    @Test
    void sortAppliesBeforeMaxFilesTruncation() throws Exception {
        // smb.Trigger does not extend vfs.Trigger, so it has its own evaluate()/sort/truncate logic
        // that needs its own coverage, unlike ftp/ftps/sftp whose Trigger is a thin vfs.Trigger subclass.
        String dir = SmbUtils.SHARE_NAME + "/trigger-sort-" + IdUtils.create();
        smbUtils.upload(dir + "/b.txt");
        smbUtils.upload(dir + "/a.txt");
        smbUtils.upload(dir + "/c.txt");

        var trigger = io.kestra.plugin.fs.smb.Trigger.builder()
            .id("smb-trigger-sort-" + UUID.randomUUID())
            .type(io.kestra.plugin.fs.smb.Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(Downloads.Action.NONE))
            .sort(Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.NAME_DESC))
            .maxFiles(Property.ofValue(2))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = ((PollingTriggerInterface) trigger).evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> files = (java.util.List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));
        assertThat(files.stream().map(file -> (String) file.get("name")).toList(), contains("c.txt", "b.txt"));

        smbUtils.delete(dir + "/a.txt");
        smbUtils.delete(dir + "/b.txt");
        smbUtils.delete(dir + "/c.txt");
    }
}
