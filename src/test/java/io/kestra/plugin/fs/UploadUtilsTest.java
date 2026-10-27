package io.kestra.plugin.fs;

import io.kestra.core.utils.FileUtils;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UploadUtilsTest {

    @Test
    void shouldPassWhenUniqueDestinationFilenames() {
        var mappings = List.<Map.Entry<String, String>>of(
            new SimpleEntry<>("file1.csv", "kestra:///path/to/a.csv"),
            new SimpleEntry<>("file2.csv", "kestra:///path/to/b.csv")
        );

        assertDoesNotThrow(() -> UploadUtils.validateUniqueDestinationFilenames(mappings));
    }

    @Test
    void shouldPassWhenUniqueDerivedFilenames() {
        var mappings = List.<Map.Entry<String, String>>of(
            new SimpleEntry<>(null, "kestra:///path/to/first.csv"),
            new SimpleEntry<>(null, "kestra:///other/path/second.csv")
        );

        assertDoesNotThrow(() -> UploadUtils.validateUniqueDestinationFilenames(mappings));
    }

    @Test
    void shouldFailWhenDuplicateExplicitDestinationFilenames() {
        var mappings = List.<Map.Entry<String, String>>of(
            new SimpleEntry<>("data.csv", "kestra:///path/to/first.csv"),
            new SimpleEntry<>("data.csv", "kestra:///path/to/second.csv")
        );

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadUtils.validateUniqueDestinationFilenames(mappings)
        );

        assertThat(exception.getMessage(), containsString("Multiple files resolve to the same destination filename 'data.csv'"));
    }

    @Test
    void shouldFailWhenDuplicateDerivedDestinationFilenames() {
        var mappings = List.<Map.Entry<String, String>>of(
            new SimpleEntry<>(null, "kestra:///path/step1/report.csv"),
            new SimpleEntry<>(null, "kestra:///path/step2/report.csv")
        );

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadUtils.validateUniqueDestinationFilenames(mappings)
        );

        assertThat(exception.getMessage(), containsString("Multiple files resolve to the same destination filename 'report.csv'"));
    }

    @Test
    void shouldSupportCustomNameExtractor() {
        var mappings = List.<Map.Entry<String, String>>of(
            new SimpleEntry<>(null, "kestra:///path/step1/r%2Ecsv"),
            new SimpleEntry<>(null, "kestra:///path/step2/r.csv")
        );

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadUtils.validateUniqueDestinationFilenames(
                mappings,
                uri -> FileUtils.getFileName(URI.create(uri))
            )
        );

        assertThat(exception.getMessage(), containsString("Multiple files resolve to the same destination filename 'r.csv'"));
    }

    @Test
    void shouldPassWhenEmptyOrNull() {
        assertDoesNotThrow(() -> UploadUtils.validateUniqueDestinationFilenames(null));
        assertDoesNotThrow(() -> UploadUtils.validateUniqueDestinationFilenames(List.of()));
    }
}
