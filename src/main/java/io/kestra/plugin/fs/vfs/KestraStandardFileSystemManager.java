package io.kestra.plugin.fs.vfs;

import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.VfsComponent;
import org.apache.commons.vfs2.provider.ftp.FtpFileNameParser;
import org.apache.commons.vfs2.provider.ftp.FtpFileProvider;
import org.apache.commons.vfs2.provider.ftps.FtpsFileProvider;
import org.apache.commons.vfs2.provider.sftp.SftpFileNameParser;
import org.apache.commons.vfs2.provider.sftp.SftpFileProvider;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class KestraStandardFileSystemManager extends StandardFileSystemManager {
    static final String CONFIG_RESOURCE = "providers.xml"; // same as StandardFileSystemManager.CONFIG_RESOURCE

    private final RunContext runContext;

    /**
     * Providers registered via {@link #addProvider(String[], FileProvider)} (which
     * {@code StandardFileSystemManager.configure} uses for every provider element).
     * Re-closed on every {@link #close()} so a file system registered by a connect that was
     * still in flight during the first close is still disconnected by the worker thread's
     * trailing try-with-resources close.
     */
    private final List<FileProvider> registeredProviders = new CopyOnWriteArrayList<>();

    KestraStandardFileSystemManager(RunContext runContext) {
        super();

        this.runContext = runContext;
    }

    @Override
    public void addProvider(String[] urlSchemes, FileProvider provider) throws FileSystemException {
        var effectiveProvider = bracketSafe(provider);
        super.addProvider(urlSchemes, effectiveProvider);
        registeredProviders.add(effectiveProvider);
    }

    /**
     * Swaps the stock SFTP/FTP/FTPS providers for subclasses whose file-name parser keeps
     * {@code [} and {@code ]} percent-encoded in VFS file names.
     * <p>
     * Commons VFS 2.10.0 {@code UriParser.decode} treats everything between {@code [} and {@code ]}
     * as an IPv6 host and skips the {@code %XX} sequences there. Every path is decoded with it
     * ({@code getPathDecoded()}, and the SFTP/FTP {@code relPath} sent to the server), so a name
     * such as {@code report[100%].txt} was turned into {@code report[100%25].txt}: it was listed
     * with the wrong name, could not be downloaded, moved or deleted, and was uploaded under the
     * wrong name. With the brackets kept as {@code %5B}/{@code %5D}, no literal bracket reaches
     * {@code decode}, so it restores the real name. Exact class checks keep any other subclass as is.
     */
    private static FileProvider bracketSafe(FileProvider provider) {
        if (provider.getClass() == SftpFileProvider.class) {
            return new BracketSafeSftpFileProvider();
        }
        if (provider.getClass() == FtpsFileProvider.class) {
            return new BracketSafeFtpsFileProvider();
        }
        if (provider.getClass() == FtpFileProvider.class) {
            return new BracketSafeFtpFileProvider();
        }
        return provider;
    }

    private static boolean isBracket(char ch) {
        return ch == '[' || ch == ']';
    }

    private static final class BracketSafeSftpFileProvider extends SftpFileProvider {
        BracketSafeSftpFileProvider() {
            setFileNameParser(new SftpFileNameParser() {
                @Override
                public boolean encodeCharacter(char ch) {
                    return super.encodeCharacter(ch) || isBracket(ch);
                }
            });
        }
    }

    private static final class BracketSafeFtpFileProvider extends FtpFileProvider {
        BracketSafeFtpFileProvider() {
            setFileNameParser(new BracketSafeFtpFileNameParser());
        }
    }

    private static final class BracketSafeFtpsFileProvider extends FtpsFileProvider {
        BracketSafeFtpsFileProvider() {
            setFileNameParser(new BracketSafeFtpFileNameParser());
        }
    }

    private static final class BracketSafeFtpFileNameParser extends FtpFileNameParser {
        @Override
        public boolean encodeCharacter(char ch) {
            return super.encodeCharacter(ch) || isBracket(ch);
        }
    }

    /**
     * Thread-safe and repeatable close, callable concurrently from the worker thread
     * (try-with-resources in {@code Trigger.evaluate}) and the killer thread
     * ({@code Trigger.kill}). {@code DefaultFileSystemManager.close()} in Commons VFS 2.10.0 is
     * neither synchronized nor idempotent-safe: concurrent closes can corrupt the provider
     * {@code HashMap}, and a second close is a no-op ({@code init == false}) that would miss a
     * file system registered after the first close by a still-establishing connection.
     * Serializing the close and re-closing the tracked providers lets the trailing worker-thread
     * close clean up such late-registered sessions instead of leaking them.
     */
    @Override
    public synchronized void close() {
        try {
            super.close(); // no-op after the first call (init == false)
        } finally {
            // Re-close providers so late-registered file systems are disconnected too.
            // AbstractFileProvider.close() snapshots and clears its components, so calling it
            // again is safe. Continue past individual failures so one bad provider cannot
            // prevent the remaining ones from being cleaned up.
            for (FileProvider provider : registeredProviders) {
                if (provider instanceof VfsComponent component) {
                    try {
                        component.close();
                    } catch (Exception ignored) {
                        // best-effort cleanup: keep closing the remaining providers
                    }
                }
            }
        }
    }

    @Override
    protected DefaultFileReplicator createDefaultFileReplicator() {
        // By default, the file replicator uses /tmp as the base temp directory; we create it manually to use the task working directory.
        File vfsCache = this.runContext.workingDir().resolve(Path.of("vfs_cache")).toFile();
        if (!vfsCache.mkdirs()) {
            throw new RuntimeException("Unable to create directory " + vfsCache.getPath());
        }

        return new DefaultFileReplicator(vfsCache);
    }
}
