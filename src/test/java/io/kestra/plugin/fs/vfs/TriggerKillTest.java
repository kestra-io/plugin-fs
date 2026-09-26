package io.kestra.plugin.fs.vfs;

import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Lifecycle tests for the polling-trigger {@code kill()}/{@code stop()} handling.
 *
 * <p>These tests don't need a live server: they verify the contract around the tracked
 * {@link StandardFileSystemManager} — no-op safety when idle, idempotency, single close,
 * and no cross-talk between consecutive polls of the same trigger instance.
 */
class TriggerKillTest {

    // No test helper subclass: Kestra's @Plugin is @Inherited all the way down from core's
    // AbstractTrigger, so ANY concrete trigger subclass in test sources would be registered as a
    // plugin service provider and break plugin scanning (private ones fatally). Reuse the existing
    // public sftp.Trigger directly instead.
    private static Trigger newTrigger() {
        return new io.kestra.plugin.fs.sftp.Trigger();
    }

    private static class CloseTrackingFileSystemManager extends StandardFileSystemManager {
        final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
            super.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<StandardFileSystemManager> tracked(Trigger trigger) throws Exception {
        Field field = Trigger.class.getDeclaredField("trackedFileSystemManager");
        field.setAccessible(true);
        return (AtomicReference<StandardFileSystemManager>) field.get(trigger);
    }

    @Test
    void killWhenNothingRunningIsNoOp() {
        Trigger trigger = newTrigger();
        trigger.kill();
        trigger.stop();
    }

    @Test
    void repeatedKillAndStopAreSafe() {
        Trigger trigger = newTrigger();
        trigger.kill();
        trigger.kill();
        trigger.stop();
        trigger.stop();
        trigger.kill();
    }

    @Test
    void killClosesTrackedManagerExactlyOnceAndClearsReference() throws Exception {
        Trigger trigger = newTrigger();
        CloseTrackingFileSystemManager fsm = new CloseTrackingFileSystemManager();
        tracked(trigger).set(fsm);

        trigger.kill();

        assertThat(fsm.closeCount.get(), is(1));
        assertThat(tracked(trigger).get() == null, is(true));

        // repeated kill must not close again
        trigger.kill();
        trigger.stop();
        assertThat(fsm.closeCount.get(), is(1));
    }

    @Test
    void stopDelegatesToSameCancellationMechanism() throws Exception {
        Trigger trigger = newTrigger();
        CloseTrackingFileSystemManager fsm = new CloseTrackingFileSystemManager();
        tracked(trigger).set(fsm);

        trigger.stop();

        assertThat(fsm.closeCount.get(), is(1));
        assertThat(tracked(trigger).get() == null, is(true));
    }

    @Test
    void killDoesNotAffectSubsequentPollManager() throws Exception {
        Trigger trigger = newTrigger();
        CloseTrackingFileSystemManager first = new CloseTrackingFileSystemManager();
        tracked(trigger).set(first);
        trigger.kill();
        assertThat(first.closeCount.get(), is(1));

        // next poll publishes its own manager; killing it must not touch the previous one
        CloseTrackingFileSystemManager second = new CloseTrackingFileSystemManager();
        tracked(trigger).set(second);
        trigger.kill();

        assertThat(first.closeCount.get(), is(1));
        assertThat(second.closeCount.get(), is(1));
        assertThat(tracked(trigger).get() == null, is(true));
    }

    @Test
    void concurrentKillClosesOnlyOnce() throws Exception {
        Trigger trigger = newTrigger();
        CloseTrackingFileSystemManager fsm = new CloseTrackingFileSystemManager();
        tracked(trigger).set(fsm);

        Thread[] killers = new Thread[8];
        for (int i = 0; i < killers.length; i++) {
            killers[i] = Thread.ofPlatform().start(trigger::kill);
        }
        for (Thread killer : killers) {
            killer.join(10_000);
        }

        assertThat(fsm.closeCount.get(), is(1));
        assertThat(tracked(trigger).get() == null, is(true));
    }
}
