package io.kestra.plugin.fs.smb;

import io.kestra.plugin.fs.vfs.models.File;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SmbServiceTest {

    @Test
    void resolveDestinationPathPreservesSubdirectories() {
        var dest1 = SmbService.resolveDestinationPath("/upload/in/sub1/data.csv", "/upload/archive/", "/upload/in/");
        assertThat(dest1, is("/upload/archive/sub1/data.csv"));

        var dest2 = SmbService.resolveDestinationPath("/upload/in/sub2/data.csv", "/upload/archive/", "/upload/in/");
        assertThat(dest2, is("/upload/archive/sub2/data.csv"));

        var destRoot = SmbService.resolveDestinationPath("/upload/in/data.csv", "/upload/archive/", "/upload/in/");
        assertThat(destRoot, is("/upload/archive/data.csv"));
    }

    @Test
    void resolveDestinationPathNormalizesSlashes() {
        var dest = SmbService.resolveDestinationPath("upload/in/sub/report.csv", "upload/archive", "upload/in");
        assertThat(dest, is("upload/archive/sub/report.csv"));

        var destWithLeading = SmbService.resolveDestinationPath("/upload/in/sub/report.csv", "/upload/archive", "upload/in");
        assertThat(destWithLeading, is("/upload/archive/sub/report.csv"));
    }

    @Test
    void resolveDestinationPathWithoutBaseDirectoryFallsBackToFilename() {
        var destNull = SmbService.resolveDestinationPath("/upload/in/data.csv", "/upload/archive", null);
        assertThat(destNull, is("/upload/archive/data.csv"));

        var destBlank = SmbService.resolveDestinationPath("/upload/in/data.csv", "/upload/archive/", "");
        assertThat(destBlank, is("/upload/archive/data.csv"));
    }

    @Test
    void relativePathPreservesSubdirectories() {
        var file1 = File.builder()
            .name("data.csv")
            .serverPath(URI.create("smb://localhost/upload/in/sub1/data.csv"))
            .build();
        var file2 = File.builder()
            .name("data.csv")
            .serverPath(URI.create("smb://localhost/upload/in/sub2/data.csv"))
            .build();
        var directFile = File.builder()
            .name("data.csv")
            .serverPath(URI.create("smb://localhost/upload/in/data.csv"))
            .build();

        assertThat(Downloads.relativePath("/upload/in/", file1), is("sub1/data.csv"));
        assertThat(Downloads.relativePath("/upload/in/", file2), is("sub2/data.csv"));
        assertThat(Downloads.relativePath("/upload/in/", directFile), is("data.csv"));
    }

    @Test
    void performActionThrowsOnCollidingDestinations() {
        var file1 = File.builder()
            .serverPath(URI.create("smb://localhost/upload/in/sub1/data.csv"))
            .build();
        var file2 = File.builder()
            .serverPath(URI.create("smb://localhost/upload/in/sub2/data.csv"))
            .build();

        assertThrows(IllegalStateException.class, () ->
            SmbService.performAction(null, null, null, List.of(file1, file2), io.kestra.plugin.fs.vfs.Downloads.Action.MOVE, "/upload/archive/", null)
        );
    }

    @Test
    void performActionThrowsWhenMoveDirectoryIsMissingOnMove() {
        var file = File.builder()
            .serverPath(URI.create("smb://localhost/upload/in/data.csv"))
            .build();

        assertThrows(IllegalArgumentException.class, () ->
            SmbService.performAction(null, null, null, List.of(file), io.kestra.plugin.fs.vfs.Downloads.Action.MOVE, null, null)
        );
    }
    @Test
    void relativePathNormalizesDotAndSlashes() {
        var file1 = File.builder()
            .name("data.csv")
            .serverPath(URI.create("smb://localhost/upload/in/sub1/data.csv"))
            .build();

        assertThat(Downloads.relativePath("/upload/in/../in/", file1), is("sub1/data.csv"));
        assertThat(Downloads.relativePath("/upload//in/", file1), is("sub1/data.csv"));
        assertThat(Downloads.relativePath("/upload/in/./", file1), is("sub1/data.csv"));
        assertThat(Downloads.relativePath("upload/in", file1), is("sub1/data.csv"));
    }

    @Test
    void resolveDestinationPathWithUnnormalizedBase() {
        var dest1 = SmbService.resolveDestinationPath("/upload/in/sub1/data.csv", "/upload/archive/", "/upload/in/../in");
        assertThat(dest1, is("/upload/archive/sub1/data.csv"));

        var dest2 = SmbService.resolveDestinationPath("/upload/in/sub1/data.csv", "/upload/archive/", "/upload//in/");
        assertThat(dest2, is("/upload/archive/sub1/data.csv"));
    }
}
