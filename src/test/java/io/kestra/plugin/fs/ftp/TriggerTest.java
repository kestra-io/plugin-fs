package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import jakarta.inject.Inject;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;

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

    @Override
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
}
