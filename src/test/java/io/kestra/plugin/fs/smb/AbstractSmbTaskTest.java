package io.kestra.plugin.fs.smb;

import io.kestra.core.models.property.Property;
import io.kestra.core.utils.IdUtils;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

class AbstractSmbTaskTest {

    @Test
    void toStringShouldNotLeakCredentials() {
        String secretPassword = "S3cr3t-P@ssw0rd!";

        TestTask task = TestTask.builder()
            .id(IdUtils.create())
            .type(TestTask.class.getName())
            .host(Property.ofValue("localhost"))
            .username(Property.ofValue("admin"))
            .password(Property.ofValue(secretPassword))
            .build();

        String toString = task.toString();

        assertThat(toString, not(containsString(secretPassword)));
        assertThat(toString, not(containsString("admin")));
    }

    // Deliberately declares no @ToString: it must inherit AbstractSmbTask's own generated
    // toString() so this test actually exercises the credential-masking fix.
    @SuperBuilder
    @NoArgsConstructor
    public static class TestTask extends AbstractSmbTask {
    }
}
