package io.kestra.plugin.fs.nfs;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class ListComparatorTest {
    @Test
    void nullLastModifiedTimeSortsLastAscending() {
        List.File dated = List.File.builder().name("a.txt").lastModifiedTime(Instant.now()).build();
        List.File undated = List.File.builder().name("b.txt").lastModifiedTime(null).build();

        Comparator<List.File> comparator = List.comparator(List.Sort.LAST_MODIFIED_ASC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(List.File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }

    @Test
    void nullLastModifiedTimeSortsLastDescending() {
        List.File dated = List.File.builder().name("a.txt").lastModifiedTime(Instant.now()).build();
        List.File undated = List.File.builder().name("b.txt").lastModifiedTime(null).build();

        Comparator<List.File> comparator = List.comparator(List.Sort.LAST_MODIFIED_DESC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(List.File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }
}
