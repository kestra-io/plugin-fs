package io.kestra.plugin.fs.smb;

import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import jakarta.inject.Inject;

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
}
