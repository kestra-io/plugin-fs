package io.kestra.plugin.fs.ftps;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;

public interface FtpsInterface {
    @Schema(
        title = "Sets FTPS mode, either \"implicit\" or \"explicit\"."
    )
    Property<FtpsMode> getMode();


    @Schema(
        title = "Sets the data channel protection level (PROT)."
    )
    Property<FtpsDataChannelProtectionLevel> getDataChannelProtectionLevel();

    @Schema(
        title = "Whether the client should disable checking of the remote SSL certificate.",
        description = "Note: This makes the SSL connection insecure, and should only be used for testing."
    )
    Property<Boolean> getInsecureTrustAllCertificates();
}
