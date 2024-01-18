package io.kestra.plugin.fs.ftp;

import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import jakarta.inject.Inject;

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
}
