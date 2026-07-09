package io.kestra.plugin.fs.local;

import io.kestra.plugin.fs.local.models.File;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class ListComparatorTest {
    @Test
    void nullModifiedDateSortsLastAscending() {
        File dated = File.builder().name("a.txt").modifiedDate(Instant.now()).build();
        File undated = File.builder().name("b.txt").modifiedDate(null).build();

        Comparator<File> comparator = List.comparator(List.Sort.LAST_MODIFIED_ASC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }

    @Test
    void nullModifiedDateSortsLastDescending() {
        File dated = File.builder().name("a.txt").modifiedDate(Instant.now()).build();
        File undated = File.builder().name("b.txt").modifiedDate(null).build();

        Comparator<File> comparator = List.comparator(List.Sort.LAST_MODIFIED_DESC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }
}
