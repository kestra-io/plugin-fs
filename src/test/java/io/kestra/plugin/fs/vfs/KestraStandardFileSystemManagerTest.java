package io.kestra.plugin.fs.vfs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.NameScope;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// Parser wiring only, no server: catches a VFS upgrade that would bypass the bracket-safe parsers (#405)
@KestraTest
class KestraStandardFileSystemManagerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "sftp|report[100%].txt",
        "ftp|report[100%].txt",
        "ftps|report[100%].txt",
        "sftp|x]y[z%41.txt",
        "ftp|x]y[z%41.txt",
        "ftps|x]y[z%41.txt",
        "sftp|a b%c+d é.txt",
        "ftp|a b%c+d é.txt",
    })
    void shouldDecodeResolvedAndChildNamesToTheRealName(String scheme, String fileName) throws Exception {
        try (var fsm = new KestraStandardFileSystemManager(runContextFactory.of())) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            var resolved = fsm.resolveURI(VfsService.uri(scheme, "localhost", 21, null, null, "/dir/" + fileName).toString());
            assertThat(resolved.getPathDecoded(), is("/dir/" + fileName));

            // a listed child is resolved the way AbstractFileObject.getChildren() does it
            var parent = fsm.resolveURI(VfsService.uri(scheme, "localhost", 21, null, null, "/dir").toString());
            var child = fsm.resolveName(parent, "./" + UriParser.encode(fileName), NameScope.CHILD);
            assertThat(child.getPathDecoded(), is("/dir/" + fileName));
            assertThat(UriParser.decode(child.getBaseName()), is(fileName));
        }
    }
}
