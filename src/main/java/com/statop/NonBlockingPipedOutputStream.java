package com.statop;

import java.io.IOException;
import java.io.OutputStream;

/**
 * See {@link com.obs.utils.io.NonBlockingPipedInputStream}
 *
 * @author Patrick Staton
 */
public class NonBlockingPipedOutputStream extends OutputStream {

    private final NonBlockingPipedInputStream input;

    NonBlockingPipedOutputStream(final NonBlockingPipedInputStream input) {
        super();
        this.input = input;
    }

    @Override
    public void write(final int b) throws IOException {
        input.write(b);
    }

    /**
     * If the size of b exceeds the buffer size, then this will block, at least, until the part of the array
     * greater than the buffer size is written. If it is only slightly smaller than the buffer size, then
     * it is very likely to block simply because it must wait for all other writes to finish.
     *
     * {@inheritDoc}
     */
    @Override
    public void write(final byte[] b) throws IOException {
        input.write(b);
    }

    /**
     * If len exceeds the buffer size, then this will block, at least, until the part of the array
     * greater than the buffer size is written. If it is only slightly smaller than the buffer size, then
     * it is very likely to block simply because it must wait for all other writes to finish.
     *
     * {@inheritDoc}
     */
    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        input.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        input.flush();
    }

    @Override
    public void close() throws IOException {
        input.closeOut();
    }
}
