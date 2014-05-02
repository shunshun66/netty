/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A {@link Timer} optimized for approximated I/O timeout scheduling.
 *
 * <h3>Tick Duration</h3>
 *
 * As described with 'approximated', this timer does not execute the scheduled
 * {@link TimerTask} on time.  {@link HashedWheelTimer}, on every tick, will
 * check if there are any {@link TimerTask}s behind the schedule and execute
 * them.
 * <p>
 * You can increase or decrease the accuracy of the execution timing by
 * specifying smaller or larger tick duration in the constructor.  In most
 * network applications, I/O timeout does not need to be accurate.  Therefore,
 * the default tick duration is 100 milliseconds and you will not need to try
 * different configurations in most cases.
 *
 * <h3>Ticks per Wheel (Wheel Size)</h3>
 *
 * {@link HashedWheelTimer} maintains a data structure called 'wheel'.
 * To put simply, a wheel is a hash table of {@link TimerTask}s whose hash
 * function is 'dead line of the task'.  The default number of ticks per wheel
 * (i.e. the size of the wheel) is 512.  You could specify a larger value
 * if you are going to schedule a lot of timeouts.
 *
 * <h3>Do not create many instances.</h3>
 *
 * {@link HashedWheelTimer} creates a new thread whenever it is instantiated and
 * started.  Therefore, you should make sure to create only one instance and
 * share it across your application.  One of the common mistakes, that makes
 * your application unresponsive, is to create a new instance for every connection.
 *
 * <h3>Implementation Details</h3>
 *
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 */
public class HashedWheelTimer implements Timer {

    static final InternalLogger logger =
            InternalLoggerFactory.getInstance(HashedWheelTimer.class);

    private static final ResourceLeakDetector<HashedWheelTimer> leakDetector =
            new ResourceLeakDetector<HashedWheelTimer>(
                    HashedWheelTimer.class, 1, Runtime.getRuntime().availableProcessors() * 4);

    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER;
    private static final AtomicLongFieldUpdater<HashedWheelTimer> TICK_UPDATER;
    static {
        AtomicIntegerFieldUpdater<HashedWheelTimer> workerStateUpdater =
                PlatformDependent.newAtomicIntegerFieldUpdater(HashedWheelTimer.class, "workerState");
        if (workerStateUpdater == null) {
            workerStateUpdater = AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");
        }
        WORKER_STATE_UPDATER = workerStateUpdater;

        AtomicLongFieldUpdater<HashedWheelTimer> tickUpdater =
                PlatformDependent.newAtomicLongFieldUpdater(HashedWheelTimer.class, "tick");
        if (tickUpdater == null) {
            tickUpdater = AtomicLongFieldUpdater.newUpdater(HashedWheelTimer.class, "tick");
        }
        TICK_UPDATER = tickUpdater;
    }

    private final ResourceLeak leak;
    private final Worker worker = new Worker();
    private final Thread workerThread;

    public static final int WORKER_STATE_INIT = 0;
    public static final int WORKER_STATE_STARTED = 1;
    public static final int WORKER_STATE_SHUTDOWN = 2;
    @SuppressWarnings({ "unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
    private volatile int workerState = WORKER_STATE_INIT; // 0 - init, 1 - started, 2 - shut down

    private final long tickDuration;
    private final HashedWheelBucket[] wheel;
    private final int mask;
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    private volatile long startTime;

    @SuppressWarnings("unused")
    private volatile long tick;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is <= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @param ticksPerWheel  the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is <= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory  a {@link ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory  a {@link ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is <= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory  a {@link ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @param ticksPerWheel  the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is <= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        workerThread = threadFactory.newThread(worker);

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        wheel = createWheel(ticksPerWheel, workerThread);
        mask = wheel.length - 1;

        // Convert tickDuration to nanos.
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow.
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
                    tickDuration, Long.MAX_VALUE / wheel.length));
        }

        leak = leakDetector.open(this);
    }

    @SuppressWarnings("unchecked")
    private static HashedWheelBucket[] createWheel(int ticksPerWheel, Thread workerThread) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException(
                    "ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i ++) {
            wheel[i] = new HashedWheelBucket(workerThread);
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            WORKER_STATE_UPDATER.set(this, WORKER_STATE_SHUTDOWN);

            if (leak != null) {
                leak.close();
            }

            return Collections.emptySet();
        }

        boolean interrupted = false;
        while (workerThread.isAlive()) {
            workerThread.interrupt();
            try {
                workerThread.join(100);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        if (leak != null) {
            leak.close();
        }
        return worker.unprocessedTimeouts();
    }

    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        start();

        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;
        long calculated = deadline / tickDuration;

        for (;;) {
            long t = TICK_UPDATER.get(this);
            long remainingRounds = (calculated - t) / wheel.length;

            // Add the timeout to the wheel.
            HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline, remainingRounds);

            final long ticks = Math.max(calculated, t); // Ensure we don't schedule for past.
            int stopIndex = (int) (ticks & mask);

            HashedWheelBucket bucket = wheel[stopIndex];
            if (bucket.add(timeout)) {
                // we was able to add to the bucket as it was not in progress atm. If not we need to try again
                return timeout;
            }
        }
    }

    private final class Worker implements Runnable {
        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();

        @Override
        public void run() {
            // Initialize the startTime.
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    HashedWheelBucket bucket =
                            wheel[(int) (TICK_UPDATER.getAndIncrement(HashedWheelTimer.this) & mask)];
                    bucket.expireTimeouts(deadline);
                }
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            for (HashedWheelBucket bucket: wheel) {
                bucket.clear(unprocessedTimeouts);
            }
        }

        /**
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            long deadline = tickDuration * (TICK_UPDATER.get(HashedWheelTimer.this) + 1);

            for (;;) {
                final long currentTime = System.nanoTime() - startTime;
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                if (PlatformDependent.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException e) {
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    private static final class HashedWheelTimeout extends HashedWheelTimeoutNode {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER;

        static {
            AtomicIntegerFieldUpdater<HashedWheelTimeout> updater =
                    PlatformDependent.newAtomicIntegerFieldUpdater(HashedWheelTimeout.class, "state");
            if (updater == null) {
                updater = AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");
            }
            STATE_UPDATER = updater;
        }

        private final HashedWheelTimer timer;
        private final TimerTask task;
        private final long deadline;
        private long remainingRounds;

        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int state = ST_INIT;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline, long remainingRounds) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
            this.remainingRounds = remainingRounds;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!STATE_UPDATER.compareAndSet(this, ST_INIT, ST_CANCELLED)) {
                return false;
            }
            return true;
        }

        @Override
        public boolean isCancelled() {
            return STATE_UPDATER.get(this) == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return STATE_UPDATER.get(this) != ST_INIT;
        }

        @Override
        public void expire() {
            if (!STATE_UPDATER.compareAndSet(this, ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        long deadline() {
            return deadline;
        }

        @Override
        long remainingRounds() {
            return remainingRounds;
        }

        @Override
        void decrementRemainingRounds() {
            remainingRounds --;
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192);
            buf.append(StringUtil.simpleClassName(this));
            buf.append('(');

            buf.append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining);
                buf.append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining);
                buf.append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            buf.append(", task: ");
            buf.append(task());

            return buf.append(')').toString();
        }
    }

    /**
     * Hashed-Wheel-Bucket for MPSC work-pattern.
     */
    @SuppressWarnings("serial")
    static final class HashedWheelBucket extends AtomicReference<HashedWheelTimeoutNode> {
        private static final AtomicReferenceFieldUpdater<HashedWheelBucket, HashedWheelTimeoutNode> TAIL_UPDATER;

        static {
            AtomicReferenceFieldUpdater<HashedWheelBucket, HashedWheelTimeoutNode> tailUpdater =
                    PlatformDependent.newAtomicReferenceFieldUpdater(HashedWheelBucket.class, "tail");
            if (tailUpdater == null) {
                tailUpdater = AtomicReferenceFieldUpdater.newUpdater(
                        HashedWheelBucket.class, HashedWheelTimeoutNode.class, "tail");
            }
            TAIL_UPDATER = tailUpdater;
        }

        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        // just used for asserts
        private final Thread workerThread;
        @SuppressWarnings({ "unused", "FieldMayBeFinal" })
        private volatile HashedWheelTimeoutNode tail;

        HashedWheelBucket(Thread workerThread) {
            this.workerThread = workerThread;
            HashedWheelTimeoutNode node = new HashedWheelTimeoutNode();
            tail = node;
            set(node);
        }

        /**
         * Add {@link HashedWheelTimeoutNode} to this bucket.
         */
        public boolean add(HashedWheelTimeoutNode timeout) {
            timeout.setNext(null);
            Lock l = lock.readLock();
            // try to lock if not possible it is hold because of expireTimeouts.
            // In this case we will try to add it to another bucket.
            if (l.tryLock()) {
                try {
                    getAndSet(timeout).setNext(timeout);
                    return true;
                } finally {
                    l.unlock();
                }
            }
            return false;
        }

        /**
         * Expire all {@link HashedWheelTimeoutNode}s for the given {@code deadline}.
         */
        public void expireTimeouts(long deadline) {
            assert Thread.currentThread() == workerThread;
            Lock l = lock.writeLock();
            try {
                l.lock();
                HashedWheelTimeoutNode last = get();

                for (;;) {
                    HashedWheelTimeoutNode node = pollNode();

                    if (node == null) {
                        // all nodes are processed
                        break;
                    }

                    if (node.remainingRounds() <= 0) {
                        if (node.deadline() <= deadline) {
                            node.expire();
                        } else {
                            // The timeout was placed into a wrong slot. This should never happen.
                            throw new Error(String.format(
                                    "timeout.deadline (%d) > deadline (%d)", node.deadline(), deadline));
                        }
                    } else if (!node.isCancelled()) {
                        // decrement and add again
                        node.decrementRemainingRounds();
                        add(node);
                    }
                    if (node == last) {
                        // We reached the node that was the last node when we started so stop here.
                        break;
                    }
                }
            } finally {
                l.unlock();
            }
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         */
        public void clear(Set<Timeout> set) {
            assert Thread.currentThread() == workerThread;
            for (;;) {
                HashedWheelTimeoutNode node = pollNode();
                if (node == null) {
                    return;
                }
                if (node.isExpired() || node.isCancelled()) {
                    continue;
                }
                set.add(node);
            }
        }

        private HashedWheelTimeoutNode pollNode() {
            HashedWheelTimeoutNode next;
            for (;;) {
                final HashedWheelTimeoutNode tail = TAIL_UPDATER.get(this);
                next = tail.next();
                if (next != null || get() == tail) {
                    break;
                }
            }
            if (next == null) {
                return null;
            }
            final HashedWheelTimeoutNode ret = next;
            // Using lazySet is good enough here as pollNode will always be called from the workerThread, which
            // basically means we have a single-consumer here.
            TAIL_UPDATER.lazySet(this, next);
            return ret;
        }
    }

    static class HashedWheelTimeoutNode implements Timeout {
        private static final AtomicReferenceFieldUpdater<HashedWheelTimeoutNode, HashedWheelTimeoutNode> NEXT_UPDATER;

        static {
            AtomicReferenceFieldUpdater<HashedWheelTimeoutNode, HashedWheelTimeoutNode> updater =
                    PlatformDependent.newAtomicReferenceFieldUpdater(HashedWheelTimeoutNode.class, "next");
            if (updater == null) {
                updater = AtomicReferenceFieldUpdater.newUpdater(
                        HashedWheelTimeoutNode.class, HashedWheelTimeoutNode.class, "next");
            }
            NEXT_UPDATER = updater;
        }

        @SuppressWarnings("unused")
        private volatile HashedWheelTimeoutNode next;

        final HashedWheelTimeoutNode next() {
            return NEXT_UPDATER.get(this);
        }

        final void setNext(HashedWheelTimeoutNode next) {
            // Using lazySet is good enough here as pollNode will always be called from the workerThread, which
            // basically means we have a single-consumer here.
            NEXT_UPDATER.lazySet(this, next);
        }

        long deadline() {
            return 0;
        }

        long remainingRounds() {
            return 1;
        }

        void decrementRemainingRounds() {
            // noop
        }

        @Override
        public Timer timer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimerTask task() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isExpired() {
            // never expires
            return false;
        }

        @Override
        public boolean isCancelled() {
            // never is cancelled
            return false;
        }

        @Override
        public boolean cancel() {
            return false;
        }

        public void expire() {
            // noop
        }
    }
}
