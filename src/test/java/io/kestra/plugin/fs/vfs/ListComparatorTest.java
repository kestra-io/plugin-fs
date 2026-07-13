package io.kestra.plugin.fs.vfs;

import io.kestra.plugin.fs.vfs.models.File;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class ListComparatorTest {
    @Test
    void nullUpdatedDateSortsLastAscending() {
        File dated = File.builder().name("a.txt").updatedDate(Instant.now()).build();
        File undated = File.builder().name("b.txt").updatedDate(null).build();

        Comparator<File> comparator = List.comparator(List.Sort.LAST_MODIFIED_ASC, File::getUpdatedDate, File::getName);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }

    @Test
    void nullUpdatedDateSortsLastDescending() {
        File dated = File.builder().name("a.txt").updatedDate(Instant.now()).build();
        File undated = File.builder().name("b.txt").updatedDate(null).build();

        Comparator<File> comparator = List.comparator(List.Sort.LAST_MODIFIED_DESC, File::getUpdatedDate, File::getName);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(File::getName).toList(),
            contains("a.txt", "b.txt")
        );
    }
}
