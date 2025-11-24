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
        title = "FTP proxy host"
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
        title = "Is the path relative to the user's home directory"
    )
    Property<Boolean> getRootDir();

    @Schema(
        title = "Whether to use a [passive mode](https://www.jscape.com/blog/active-v-s-passive-ftp-simplified). Passive mode is generally considered more secure as it's less likely to encounter issues with NAT and firewalls. Therefore, this property is by default set to `true`. To use active mode instead, set the property to `false`."
    )
    Property<Boolean> getPassiveMode();

    @Schema(
        title = "Ensure the server IP responding matches the one that received the request."
    )
    Property<Boolean> getRemoteIpVerification();

    Options getOptions();

    @Getter
    @Builder(toBuilder = true)
    @Jacksonized
    class Options {
        @Schema(
            title = "The timeout for the initial control connection."
        )
        @Builder.Default
        Property<Duration> connectionTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "The timeout for opening the data channel."
        )
        @Builder.Default
        Property<Duration> dataTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "The socket timeout."
        )
        @Builder.Default
        Property<Duration> socketTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "The control keep-alive timeout.",
            description = "Ensures the socket stays alive after downloading a large file."
        )
        @Builder.Default
        Property<Duration> controlKeepAliveTimeout = Property.ofValue(Duration.ofSeconds(30));

        @Schema(
            title = "The control keep-alive reply timeout.",
            description = "Ensures the socket stays alive after downloading a large file."
        )
        @Builder.Default
        Property<Duration> controlKeepAliveReplyTimeout = Property.ofValue(Duration.ofSeconds(30));
    }
}
