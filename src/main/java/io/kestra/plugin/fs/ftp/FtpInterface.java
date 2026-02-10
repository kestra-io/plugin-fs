package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.jackson.Jacksonized;

import java.net.Proxy;
import java.time.Duration;

public interface FtpInterface {
    @Schema(
        title = "FTP proxy hostname"
    )
    Property<String> getProxyHost();

    @Schema(
        title = "FTP proxy port"
    )
    Property<String> getProxyPort();

    @Schema(
        title = "FTP proxy type"
    )
    Property<Proxy.Type> getProxyType();

    @Schema(
        title = "Treat path as user home root",
        description = "If true (default), remote paths are resolved relative to the authenticated user's home directory."
    )
    Property<Boolean> getRootDir();

    @Schema(
        title = "Use passive data connections",
        description = "Passive mode avoids most firewall and NAT issues and is enabled by default (`true`). Set to `false` to force active mode."
    )
    Property<Boolean> getPassiveMode();

    @Schema(
        title = "Verify data channel IP",
        description = "Ensures the responding server IP matches the control connection to prevent spoofed data channels."
    )
    Property<Boolean> getRemoteIpVerification();

    Options getOptions();

    @Getter
    @Builder(toBuilder = true)
    @Jacksonized
    class Options {
        @Schema(
            title = "Control connection timeout",
            description = "Maximum time to open the control channel. Default 30s."
        )
        @Builder.Default
        Property<Duration> connectionTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "Data channel timeout",
            description = "Maximum time to open the data socket. Default 30s."
        )
        @Builder.Default
        Property<Duration> dataTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "Socket read timeout",
            description = "Timeout for socket reads and writes once connected. Default 30s."
        )
        @Builder.Default
        Property<Duration> socketTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "Control keep-alive interval",
            description = "Sends keep-alive commands during long transfers. Default 30s."
        )
        @Builder.Default
        Property<Duration> controlKeepAliveTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "Keep-alive reply timeout",
            description = "How long to wait for keep-alive responses before failing. Default 30s."
        )
        @Builder.Default
        Property<Duration> controlKeepAliveReplyTimeout = Property.ofValue(Duration.ofSeconds(30));
    }
}
