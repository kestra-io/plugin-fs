package io.kestra.plugin.fs.ftps;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;

public interface FtpsInterface {
    @Schema(
        title = "Select FTPS mode",
        description = "Choose implicit or explicit FTPS. Default EXPLICIT."
    )
    Property<FtpsMode> getMode();


    @Schema(
        title = "Data channel protection level",
        description = "PROT command value (`P`, `C`, etc.). Default P (encrypted)."
    )
    Property<FtpsDataChannelProtectionLevel> getDataChannelProtectionLevel();

    @Schema(
        title = "Trust all certificates",
        description = "Skip server certificate validation. Insecure; use only for testing."
    )
    Property<Boolean> getInsecureTrustAllCertificates();
}
