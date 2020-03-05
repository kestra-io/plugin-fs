package org.kestra.task.fs.auths;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.InputProperty;

import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
abstract public class Password {
    @InputProperty(
        description = "username",
        body = "Remote username to login on the server",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    @NotNull
    protected String username;

    @InputProperty(
        description = "password",
        body = "Remote password to login on the server",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String password;
}
