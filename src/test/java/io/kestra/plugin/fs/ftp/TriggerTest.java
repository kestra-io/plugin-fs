package io.kestra.plugin.fs.ftp;

import io.kestra.plugin.fs.AbstractTriggerTest;
import io.kestra.plugin.fs.vfs.Upload;
import jakarta.inject.Inject;

class TriggerTest extends AbstractTriggerTest {
    @Inject
    private FtpUtils ftpUtils;

    @Override
    public Upload.Output upload(String to) throws Exception {
        return this.ftpUtils.upload(to);
    }
}
