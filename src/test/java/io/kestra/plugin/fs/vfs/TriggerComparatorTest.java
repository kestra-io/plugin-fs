package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.triggers.StatefulTriggerService.Entry;
import io.kestra.plugin.fs.vfs.models.File;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Comparator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class TriggerComparatorTest {
    @Test
    void nullUpdatedDateSortsLastAscending() {
        Trigger.PendingFile dated = pendingFile("a.txt", Instant.now());
        Trigger.PendingFile undated = pendingFile("b.txt", null);

        Comparator<Trigger.PendingFile> comparator = Trigger.pendingFileComparator(List.Sort.LAST_MODIFIED_ASC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(p -> p.file.getName()).toList(),
            contains("a.txt", "b.txt")
        );
    }

    @Test
    void nullUpdatedDateSortsLastDescending() {
        Trigger.PendingFile dated = pendingFile("a.txt", Instant.now());
        Trigger.PendingFile undated = pendingFile("b.txt", null);

        Comparator<Trigger.PendingFile> comparator = Trigger.pendingFileComparator(List.Sort.LAST_MODIFIED_DESC);

        assertThat(
            java.util.List.of(undated, dated).stream().sorted(comparator).map(p -> p.file.getName()).toList(),
            contains("a.txt", "b.txt")
        );
    }

    private static Trigger.PendingFile pendingFile(String name, Instant updatedDate) {
        File file = File.builder().name(name).updatedDate(updatedDate).build();
        Entry candidate = Entry.candidate("/" + name, "v", Instant.EPOCH);
        return new Trigger.PendingFile(file, candidate, Trigger.ChangeType.CREATE);
    }
}
