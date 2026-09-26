package io.kestra.plugin.fs.vfs;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractVfsComponent;
import org.apache.commons.vfs2.provider.FileProvider;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Lifecycle tests for the polling-trigger {@code kill()} handling.
 *
 * <p>Kept free of live servers and reflection: the publish/clear races are covered by the
 * deterministic SFTP integration test ({@code killBeforePollAbortsWithoutTouchingFiles}),
 * while these unit checks cover idempotency, graceful-stop semantics, and the thread-safe
 * repeatable manager close. Reuses the existing public SFTP trigger: any concrete trigger
 * subclass in test sources would be registered as a plugin and break plugin scanning.
 */
class TriggerKillTest {

    // No test helper subclass: Kestra's @Plugin is @Inherited all the way down from core's
    // AbstractTrigger, so ANY concrete trigger subclass in test sources would be registered as a
    // plugin service provider and break plugin scanning (private ones fatally). Reuse the existing
    // public sftp.Trigger directly instead.
    private static Trigger newTrigger() {
        return new io.kestra.plugin.fs.sftp.Trigger();
    }

    @Test
    void killWhenNothingRunningIsNoOp() {
        Trigger trigger = newTrigger();
        trigger.kill();
    }

    @Test
    void repeatedKillIsSafe() {
        Trigger trigger = newTrigger();
        trigger.kill();
        trigger.kill();
        trigger.kill();
    }

    @Test
    void stopDoesNotThrowAndRemainsGraceful() {
        // stop() is the graceful-shutdown signal and must not cancel the poll: it keeps the
        // default no-op so a rolling restart lets the in-flight poll finish within the grace
        // period instead of tearing down MOVE/DELETE operations.
        Trigger trigger = newTrigger();
        trigger.stop();
        trigger.stop();
        trigger.kill();
    }

    @Test
    void concurrentKillIsSafe() throws Exception {
        Trigger trigger = newTrigger();

        Thread[] killers = new Thread[8];
        for (int i = 0; i < killers.length; i++) {
            killers[i] = Thread.ofPlatform().start(trigger::kill);
        }
        for (Thread killer : killers) {
            killer.join(10_000);
        }
    }

    @Test
    void managerCloseIsThreadSafeAndRepeatable() throws Exception {
        KestraStandardFileSystemManager fsm = new KestraStandardFileSystemManager(null);
        CountingProvider provider = new CountingProvider();
        fsm.addProvider(new String[]{"kestra-test"}, provider);

        Thread[] closers = new Thread[8];
        for (int i = 0; i < closers.length; i++) {
            closers[i] = Thread.ofPlatform().start(fsm::close);
        }
        for (Thread closer : closers) {
            closer.join(10_000);
        }
        // Extra sequential closes must still re-close providers (late-registered file systems).
        fsm.close();
        fsm.close();

        // 8 concurrent + 2 sequential closes, each re-closing the tracked provider.
        assertThat(provider.closeCount.get(), is(10));
    }

    @Test
    void managerCloseCleansLateRegisteredProvidersAndContinuesPastFailures() throws Exception {
        KestraStandardFileSystemManager fsm = new KestraStandardFileSystemManager(null);
        CountingProvider first = new CountingProvider();
        fsm.addProvider(new String[]{"kestra-test-a"}, first);

        // First close (as kill() would do).
        fsm.close();
        assertThat(first.closeCount.get(), is(1));

        // A connect that was in flight during the kill registers afterwards; the trailing
        // worker-thread close must still clean it up instead of leaking the session.
        CountingProvider late = new CountingProvider();
        fsm.addProvider(new String[]{"kestra-test-b"}, late);
        FailingProvider failing = new FailingProvider();
        fsm.addProvider(new String[]{"kestra-test-c"}, failing);
        CountingProvider afterFailure = new CountingProvider();
        fsm.addProvider(new String[]{"kestra-test-d"}, afterFailure);

        fsm.close();

        assertThat(first.closeCount.get(), is(2));
        assertThat(late.closeCount.get(), is(1));
        assertThat(afterFailure.closeCount.get(), is(1));
    }

    private static class CountingProvider extends AbstractVfsComponent implements FileProvider {
        final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
            super.close();
        }

        @Override
        public FileObject createFileSystem(String scheme, FileObject file, FileSystemOptions fileSystemOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileObject findFile(FileObject baseFile, String uri, FileSystemOptions fileSystemOptions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Capability> getCapabilities() {
            return List.of();
        }

        @Override
        public org.apache.commons.vfs2.FileSystemConfigBuilder getConfigBuilder() {
            return null;
        }

        @Override
        public FileName parseUri(FileName root, String uri) throws FileSystemException {
            throw new FileSystemException("not supported in test");
        }
    }

    private static class FailingProvider extends CountingProvider {
        @Override
        public void close() {
            super.close();
            throw new RuntimeException("simulated provider close failure");
        }
    }
}
