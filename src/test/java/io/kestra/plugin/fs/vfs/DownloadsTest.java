package io.kestra.plugin.fs.vfs;

import io.kestra.plugin.fs.vfs.models.File;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class DownloadsTest {

    @Test
    void relativePathShouldPreserveSubdirectories() {
        var from = URI.create("sftp://localhost/upload/data/");

        var file1 = File.builder()
            .name("report.csv")
            .serverPath(URI.create("sftp://localhost/upload/data/subA/report.csv"))
            .build();

        var file2 = File.builder()
            .name("report.csv")
            .serverPath(URI.create("sftp://localhost/upload/data/subB/report.csv"))
            .build();

        var directFile = File.builder()
            .name("direct.csv")
            .serverPath(URI.create("sftp://localhost/upload/data/direct.csv"))
            .build();

        assertThat(Downloads.relativePath(from, file1), is("subA/report.csv"));
        assertThat(Downloads.relativePath(from, file2), is("subB/report.csv"));
        assertThat(Downloads.relativePath(from, directFile), is("direct.csv"));
    }

    @Test
    void relativePathShouldHandleFromWithoutTrailingSlash() {
        var from = URI.create("sftp://localhost/upload/data");

        var file = File.builder()
            .name("report.csv")
            .serverPath(URI.create("sftp://localhost/upload/data/subA/report.csv"))
            .build();

        assertThat(Downloads.relativePath(from, file), is("subA/report.csv"));
    }

    @Test
    void relativePathShouldFallbackToNameWhenServerPathNull() {
        var from = URI.create("sftp://localhost/upload/data");

        var file = File.builder()
            .name("report.csv")
            .build();

        assertThat(Downloads.relativePath(from, file), is("report.csv"));
    }

    @Test
    void relativePathShouldHandleNonCanonicalFrom() {
        var fromDoubleSlash = URI.create("sftp://localhost/upload//data//");
        var fromDotSlash = URI.create("sftp://localhost/upload/./data/");

        var file = File.builder()
            .name("report.csv")
            .serverPath(URI.create("sftp://localhost/upload/data/subA/report.csv"))
            .build();

        assertThat(Downloads.relativePath(fromDoubleSlash, file), is("subA/report.csv"));
        assertThat(Downloads.relativePath(fromDotSlash, file), is("subA/report.csv"));
    }
}
