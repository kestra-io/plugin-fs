package io.kestra.plugin.fs.vfs;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class VfsServiceTest {
    @Test
    void decodePathShouldDecodePercentInsideBrackets() {
        assertThat(VfsService.decodePath("/upload/report[100%25].txt"), is("/upload/report[100%].txt"));
    }

    @Test
    void decodePathShouldPreserveNakedPercentSigns() {
        assertThat(VfsService.decodePath("/upload/report[100%].txt"), is("/upload/report[100%].txt"));
    }

    @Test
    void decodePathShouldPreservePlusSigns() {
        assertThat(VfsService.decodePath("/upload/report+1%25.txt"), is("/upload/report+1%.txt"));
    }
}
