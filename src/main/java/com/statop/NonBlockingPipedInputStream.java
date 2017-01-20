package com.statop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A piped stream that does not block unless it's internal buffer fills up from the write side or is empty from the read side.
 * This implementation only supports 2 threads, one writing and one reading. Attempts to use more than one thread
 * on either end will have undefined results.
 *
 * @author Patrick Staton
 */
public class NonBlockingPipedInputStream extends InputStream {
    private static final int DEFAULT_BUFFER_SIZE = 16384;

    //we can use a normal byte array and write to portions of it in separate threads as long as only one thread at a time is accessing a given part of the array,
    //ie, the java spec explicitly prohibits "word-tearing", see http://docs.oracle.com/javase/specs/jls/se7/html/jls-17.html#jls-17.6

    //every time we pass data from on thread to the other, we do buf = bufRef2, which will force the data in the buffer
    //to be visible in the other thread

    //ie writing to the buffer in one thread might not make the content visible in the other thread
    //without doing the volatile write of the reference to the buffer

    //this does not imply that the _contents_ of the buffer is volatile, just that the reference is volatile and we have a way to force
    //content to be visible in a separate thread without a lock

    private volatile byte[] buf;
    private final byte[] bufRef2;


    //we use two "pipes", one to send "sectors" of the buffer to the read thread for reading
    private final Pipe read;
    //and the other to send free sectors of the buffer back to the write thread
    private final Pipe write;
    //the implementation of the pipe is identical for both directions

    private final NonBlockingPipedOutputStream out;

    public NonBlockingPipedInputStream() {
        this(DEFAULT_BUFFER_SIZE);
    }

    /**
     * @param bufferSize the size of the buffer in bytes, this influences how often the pipe will block.
     *                   The larger the buffer is, the faster the pipe will be.
     *                   The speedup goes down asymptotically with the size of the buffer related to the size
     *                   of the average write operation.
     */
    public NonBlockingPipedInputStream(final int bufferSize) {
        super();
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must be > 0, it was " + bufferSize);
        }
        buf = new byte[bufferSize];
        bufRef2 = buf;
        read = new Pipe(bufferSize, this);
        write = read.opposite;
        write.spaceEnd.set(write.spaceEnd.get() + bufferSize);
        read.oppositeSpaceEnd = write.spaceEnd.get();
        out = new NonBlockingPipedOutputStream(this);
    }

    public int getBufferSize() {
        return buf.length;
    }

    /**
     * @return the output stream to use when writing to this pipe.
     */
    public NonBlockingPipedOutputStream outputStream() {
        return out;
    }

    @Override
    public int read() throws IOException {
        assertOpen(true);

        if (!read.fetch()) {
            return -1;
        }

        int ret = buf[read.cur++] & 0xFF;

        read.finish();

        return ret;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] buffer, final int offset, int remaining) throws IOException {
        assertOpen(true);

        if (offset > buffer.length - remaining || offset < 0 || remaining < 0) {
            throw new IndexOutOfBoundsException();
        }

        if (remaining == 0) {
            return 0;
        }

        int copied = 0;
        int toCopy;

        byte[] bufRef = buf;

        do {
            if (!read.fetch()) {
                return -1;
            }

            toCopy = Math.min(remaining, read.end - read.cur);

            System.arraycopy(bufRef, read.cur, buffer, offset + copied, toCopy);

            read.cur += toCopy;

            read.finish();

            copied += toCopy;
            remaining -= toCopy;

        } while ((remaining > 0) && (read.spaceEnd.get() != read.spaceStart));

        return copied;
    }

    @Override
    public int available() throws IOException {
        assertOpen(true);
        long available = read.spaceEnd.get() - read.spaceStart;
        return (available > ((long) Integer.MAX_VALUE)) ? Integer.MAX_VALUE : (int) available;
    }

    void write(final int b) throws IOException {
        assertOpen(false);

        if (!write.fetch()) {
            assertOpen(false);
            return;
        }

        buf[write.cur++] = (byte) b;

        write.finish();
    }

    void write(final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    void write(final byte[] buffer, int offset, int remaining) throws IOException {
        assertOpen(false);

        if (offset < 0 || offset > buffer.length - remaining || remaining < 0) {
            throw new ArrayIndexOutOfBoundsException("invalid input buffer sizes");
        }

        int toCopy;
        byte[] bufRef = buf;

        while (remaining > 0) {

            if (!write.fetch()) {
                assertOpen(false);
                return;
            }

            toCopy = Math.min(remaining, write.end - write.cur);

            System.arraycopy(buffer, offset, bufRef, write.cur, toCopy);

            write.cur += toCopy;

            write.finish();

            remaining -= toCopy;
            offset += toCopy;
        }
    }

    void flush() throws IOException {
        assertOpen(false);
        //noop since the only thread that should call this is the write thread and
        //it should have already notified the read thread that it can read the rest of the buffer

        //PipedOutputStream also does not guarantee the entire buffer is flushed before returning from flush
        //since we cannot "force" the read thread to read the rest of the buffer and the contract of flush
        //is an "indication" only
    }

    @Override
    public void close() throws IOException {
        boolean wasClosed = read.closed;
        read.closed = true;
        if (!wasClosed) {
            write.sync.release();
        }
    }

    void closeOut() throws IOException {
        boolean wasClosed = write.closed;
        write.closed = true;
        if (!wasClosed) {
            read.sync.release();
        }
    }

    private void assertOpen(boolean input) throws IOException {
        if (input) {
            if (read.closed) {
                throw new IOException("Stream is closed");
            }
        } else if (write.closed) {
            throw new IOException("Stream is closed");
        } else if (read.closed) {
            throw new IOException("Input side of pipe is closed");
        }
    }


    private static final class Pipe {

        private final NonBlockingPipedInputStream inputStream;

        private final Pipe opposite;

        private final Semaphore sync = new Semaphore(0);
        private volatile boolean blocking = false;

        private volatile boolean closed = false;

        //we exploit the fact that we only have 2 pipes from 2 threads to implement a "pipe" without
        //any blocking and without compare-and-swap.

        private final long lengthLong;
        private final int length;

        private int start = 0;
        private int cur = 0;
        private int end = 0;

        //spaceEnd is incremented by one thread when space becomes available and spaceStart lags behind indicating to
        //the opposite thread that space is available
        private long spaceStart = 0;

        private AtomicLong spaceEnd = new AtomicLong(0);
        //what opposite.spaceEnd "should be", since we us lazySet, even the current thread might not get the same value immediately after lazySet
        private long oppositeSpaceEnd;


        private Pipe(final int length, NonBlockingPipedInputStream inputStream) {
            this.inputStream = inputStream;
            this.length = length;
            this.lengthLong = length;
            this.opposite = new Pipe(this, length, inputStream);
        }
        private Pipe(final Pipe opposite, final int length, NonBlockingPipedInputStream inputStream) {
            this.inputStream = inputStream;
            this.opposite = opposite;
            this.length = length;
            this.lengthLong = length;
        }


        private boolean fetch() throws IOException {

            if (spaceStart == spaceEnd.get()) {
                if (cur != end) {
                    start = cur;
                    return true;
                }
                do {
                    try {
                        blocking = true;
                        if (spaceStart != spaceEnd.get()) {
                            break;
                        }
                        if (closed || opposite.closed) {
                            //needed because we might get written to between the check above and checking the closed flags
                            if (spaceStart != spaceEnd.get()) {
                                break;
                            }
                            return false;
                        }
                        sync.acquire();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    } finally {
                        blocking = false;
                    }
                } while (spaceStart == spaceEnd.get());
            }

            if (cur == length) {
                end = cur = 0;
            }

            start = cur;

            //this auto-magically handles long overflow after ~9 exabytes of data
            //ie spaceEnd will be close to Long.MIN_VALUE and spaceStart will be close to Long.MAX_VALUE, subtracting Long.MAX_VALUE from Long.MIN_VALUE causes reverse-overflow back to the correct answer.
            //ie the max amount of space available will never be greater than Integer.MAX_VALUE
            //and (Long.MIN_VALUE + ((long) Integer.MAX_VALUE)) - Long.MAX_VALUE == ((long) Integer.MAX_VALUE)+ 1L, which is the correct answer
            long space = spaceEnd.get() - spaceStart;

            if (((long) end) + space > lengthLong) {
                space = length - end;
            }

            spaceStart += space;
            end += space;

            return true;
        }

        private void finish() {

            //force updated buf contents to be visible in other thread, while this may not seem necessary and I can run a whole bunch of testing that shows it's not needed,
            //the JLS non-the-less says that it is possible for updates to the buffer's contents to not be visible in the other thread without this
            inputStream.buf = inputStream.bufRef2;

            //use lazySet because this is actually the only thread updating the value, but we need an "atomic" long write that eventually will become visible in the other thread
            //ie the JLS says that non-volatile writes to long fields must be treated as two non-atomic writes
            oppositeSpaceEnd += cur - start;
            opposite.spaceEnd.lazySet(oppositeSpaceEnd);

            //since we always add to the queue before we get here and fetch() always re-checks the queue, this will work fine
            if (opposite.blocking) {
                //one of two things happens here:

                //-fetch is currently waiting in acquire(), in which case it will set blocking to false anyway after we release() and since
                //     we set it to false before we release the lock, we avoid the case where fetch returns, comes back in, sets blocking to true again,
                //     blocking gets set back to false here, and then fetch() blocks forever
                //
                //-or fetch is not going to block on acquire() because it got the new space after it set blocking to true but before acquire()
                //    in which case, we will always add at least one permit to the semaphore, so if we get in the situation described above,
                //    the loop in fetch() will get through at least one acquire() and then re-set blocking to true, which is fine

                opposite.blocking = false;
                opposite.sync.release();
            }
        }
    }





    static long speedTest(final InputStream input, final OutputStream out, final int iterations, final int bytesPerWrite, final int bytesPerRead) throws Throwable {

        final AtomicLong start = new AtomicLong();
        final AtomicLong end = new AtomicLong();

        final CyclicBarrier barrier = new CyclicBarrier(2, () -> start.set(System.nanoTime()));

        Thread writeThread = new Thread(() -> {
            try {
                byte[] buf = new byte[bytesPerWrite];

                barrier.await();

                for (int y = 0; y < iterations; ++y) {
                    out.write(buf, 0, bytesPerWrite);
                }

                out.close();
            } catch (final Throwable t) {
                t.printStackTrace();
            }
        }, "SpeedTestNonBlockingPipedInputStreamThread1");

        writeThread.setDaemon(true);
        writeThread.setPriority(Thread.MAX_PRIORITY);


        Thread readThread = new Thread(() -> {
            try {
                byte[] buf = new byte[bytesPerRead];
                int read;
                barrier.await();
                while(true) {
                    read = input.read(buf);
                    if (read < 0) {
                        break;
                    }
                }

                end.set(System.nanoTime());
            } catch (final Throwable t) {
                t.printStackTrace();
            }
        }, "SpeedTestNonBlockingPipedInputStreamThread2");

        readThread.setDaemon(true);
        readThread.setPriority(Thread.MAX_PRIORITY);

        readThread.start();
        writeThread.start();

        readThread.join();
        writeThread.join();

        return end.get() - start.get();
    }



    static void checkConsistency(final InputStream input, final OutputStream out, final int iterations, final int bytesPerWrite, final int bytesPerRead) throws Throwable {

        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread writeThread = new Thread(() -> {
            try {
                byte counter = 0;
                byte[] buf = new byte[bytesPerWrite];
                int x;

                barrier.await();
                for (int y = 0; y < iterations; ++y) {
                    for (x = 0; x < bytesPerWrite; ++x) {
                        buf[x] = ++counter;
                    }
                    out.write(buf, 0, bytesPerWrite);
                }

                out.close();
            } catch (final Throwable t) {
                t.printStackTrace();
            }
        }, "CheckConsistencyNonBlockingPipedInputStreamThread1");

        writeThread.setDaemon(true);
        writeThread.setPriority(Thread.MAX_PRIORITY);


        Thread readThread = new Thread(() -> {
            try {
                byte counter = 0;
                byte[] buf = new byte[bytesPerRead];
                int x, read;

                barrier.await();
                while(true) {
                    read = input.read(buf);
                    if (read < 0) {
                        break;
                    }
                    for (x = 0; x < read; ++x) {
                        if (buf[x] != ++counter) {
                            throw new IllegalStateException("got wrong value!");
                        }
                    }
                }

            } catch (final Throwable t) {
                t.printStackTrace();
            }
        }, "CheckConsistencyNonBlockingPipedInputStreamThread2");

        readThread.setDaemon(true);
        readThread.setPriority(Thread.MAX_PRIORITY);

        readThread.start();
        writeThread.start();

        readThread.join();
        writeThread.join();
    }
}
