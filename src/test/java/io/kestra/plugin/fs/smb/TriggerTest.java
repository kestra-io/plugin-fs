package io.kestra.plugin.fs.smb;

import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.vfs.Upload;
import jakarta.inject.Inject;

class TriggerTest extends AbstractFileTriggerTest {
    @Inject
    private SmbUtils smbUtils;

    @Override
    public Upload.Output upload(String to) throws Exception {
        return this.smbUtils.upload(to);
    }

    @Override
    protected String triggeringFlowId() {
        return "smb-listen";
    }
}
