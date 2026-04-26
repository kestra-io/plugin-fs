package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.codelibs.jcifs.smb.CIFSContext;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSmbTask extends Task implements SmbInterface {
    @NotNull
    protected Property<String> host;

    @PluginProperty(secret = true)
    protected Property<String> username;

    @PluginProperty(secret = true)
    protected Property<String> password;

    @Builder.Default
    protected Property<String> port = Property.ofValue("445");

    protected CIFSContext createContext(RunContext runContext) throws Exception {
        return SmbService.createContext(runContext, this);
    }
}
