package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

class AbstractVfsTaskTest {

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

    // Deliberately declares no @ToString: it must inherit AbstractVfsTask's own generated
    // toString() so this test actually exercises the credential-masking fix.
    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class TestTask extends AbstractVfsTask {
        @Builder.Default
        private Property<String> port = Property.ofValue("22");

        @Override
        protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
            return new FileSystemOptions();
        }

        @Override
        protected String scheme() {
            return "test";
        }
    }
}
