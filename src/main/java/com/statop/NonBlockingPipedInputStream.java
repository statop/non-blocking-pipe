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

    //we can use a normal byte array as long as only one thread at a time is accessing a given part of the array,
    //ie, the java spec explicitly prohibits "word-tearing", see http://docs.oracle.com/javase/specs/jls/se7/html/jls-17.html#jls-17.6
    private final byte[] buf;

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
        read = new Pipe(bufferSize);
        write = read.opposite;
        write.spaceEnd += bufferSize;
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

        do {
            if (!read.fetch()) {
                return -1;
            }

            toCopy = Math.min(remaining, read.end - read.cur);

            System.arraycopy(buf, read.cur, buffer, offset + copied, toCopy);

            read.cur += toCopy;

            read.finish();

            copied += toCopy;
            remaining -= toCopy;

        } while ((remaining > 0) && (read.spaceEnd != read.spaceStart));

        return copied;
    }

    @Override
    public int available() throws IOException {
        assertOpen(true);
        long available = read.spaceEnd - read.spaceStart;
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

        while (remaining > 0) {

            if (!write.fetch()) {
                assertOpen(false);
                return;
            }

            toCopy = Math.min(remaining, write.end - write.cur);

            System.arraycopy(buffer, offset, buf, write.cur, toCopy);

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

        private final Pipe opposite;

        private final Semaphore sync = new Semaphore(0);
        private volatile boolean blocking = false;

        private volatile boolean closed = false;

        //we exploit the fact that we only have 2 pipes from 2 threads to implement a "pipe" without
        //any blocking and without compare-and-swap. In fact, spaceEnd probably doesn't even need to be volatile

        private final long lengthLong;
        private final int length;

        private int start = 0;
        private int cur = 0;
        private int end = 0;

        //spaceEnd is incremented by one thread when space becomes available and spaceStart lags behind indicating to
        //the opposite thread that space is available
        private long spaceStart = 0;
        private volatile long spaceEnd = 0;


        private Pipe(final int length) {
            this.length = length;
            this.lengthLong = length;
            this.opposite = new Pipe(this, length);
        }
        private Pipe(final Pipe opposite, final int length) {
            this.opposite = opposite;
            this.length = length;
            this.lengthLong = length;
        }


        private boolean fetch() throws IOException {

            if (spaceStart == spaceEnd) {
                if (cur != end) {
                    start = cur;
                    return true;
                }
                do {
                    try {
                        blocking = true;
                        if (spaceStart != spaceEnd) {
                            break;
                        }
                        if (closed || opposite.closed) {
                            //needed because we might get written to between the check above and checking the closed flags
                            if (spaceStart != spaceEnd) {
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
                } while (spaceStart == spaceEnd);
            }

            if (cur == length) {
                end = cur = 0;
            }

            start = cur;

            //this auto-magically handles long overflow after ~9 exabytes of data
            //ie spaceEnd will be close to Long.MIN_VALUE and spaceStart will be close to Long.MAX_VALUE, subtracting Long.MAX_VALUE from Long.MIN_VALUE causes reverse-overflow back to the correct answer.
            //ie the max amount of space available will never be greater than Integer.MAX_VALUE
            //and (Long.MIN_VALUE + ((long) Integer.MAX_VALUE)) - Long.MAX_VALUE == ((long) Integer.MAX_VALUE)+ 1L, which is the correct answer
            long space = spaceEnd - spaceStart;

            if (((long) end) + space > lengthLong) {
                space = length - end;
            }

            spaceStart += space;
            end += space;

            return true;
        }

        private void finish() {

            //normally, incrementing a volatile field will not work, you need a compare-and-swap, but since only one thread is writing, this is fine
            opposite.spaceEnd = opposite.spaceEnd + (cur - start);

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





    public static long speedTest(final InputStream input, final OutputStream out, final Random ra, final Random rb, final int iterations) throws Throwable {

        final AtomicLong start = new AtomicLong();
        final AtomicLong end = new AtomicLong();

        final CyclicBarrier barrier = new CyclicBarrier(2, new Runnable() {
            @Override
            public void run() {
                start.set(System.nanoTime());
            }
        });

        Thread writeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
//                    byte[] buf = new byte[432];

                    for (int y = 0; y < iterations; ++y) {
                        if (ra.nextBoolean()) {
                            byte[] buf = new byte[ra.nextInt(1000) + 1];
//                            ra.nextBytes(buf);
                            if (ra.nextBoolean()) {
                                out.write(buf);
                            } else {
                                int off = ra.nextInt(buf.length);
                                int len = ra.nextInt(buf.length - off);
                                out.write(buf, off, len);
                            }
                        } else {
                            int i = ra.nextInt();
                            out.write(i);
                        }
                    }

                    out.close();
                } catch (final Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "SpeedTestNonBlockingPipedInputStreamThread1");

        writeThread.setDaemon(true);
        writeThread.setPriority(Thread.MAX_PRIORITY);


        Thread readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    byte[] buf = new byte[1000];
                    while(true) {
//                        if (rb.nextBoolean()) {
//                            byte[] buf = new byte[1000];
                            int read;

//                            if (rb.nextBoolean()) {
                                read = input.read(buf);
//                            } else {
//                                int off = rb.nextInt(buf.length);
//                                read = input.read(buf, off, rb.nextInt(buf.length - off));
//                            }

                            if (read < 0) {
                                break;
                            }
//
//                        } else {
//                            if (input.read() < 0) {
//                                break;
//                            }
//                        }
                    }

                    end.set(System.nanoTime());
                } catch (final Throwable t) {
                    t.printStackTrace();
                }
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
}
