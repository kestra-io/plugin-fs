package io.kestra.plugin.fs.smb;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import jakarta.inject.Inject;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;

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
}
