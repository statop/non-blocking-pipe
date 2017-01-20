package com.statop;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.TestCase;


public class NonBlockingPipedInputStreamTest extends TestCase
{

    public void testPipe() throws Throwable {


        System.out.println("speed test buffer size 1");

        NonBlockingPipedInputStream input = new NonBlockingPipedInputStream(1);

        speedTest(input, input.outputStream(), 1000, 100, 100);

        PipedInputStream pInput = new PipedInputStream(1);

        speedTest(pInput, new PipedOutputStream(pInput), 1000, 100, 100);



        System.out.println( "speed test default buffer size");

        input = new NonBlockingPipedInputStream();

        speedTest(input, input.outputStream(), 10000000, 1000, 1000);

        pInput = new PipedInputStream(input.getBufferSize());

        speedTest(pInput, new PipedOutputStream(pInput), 10000000, 1000, 1000);




        System.out.println( "speed test buffer size 1mb");

        input = new NonBlockingPipedInputStream(1048576);

        speedTest(input, input.outputStream(), 10000000, 1000, 1000);

        pInput = new PipedInputStream(1048576);

        speedTest(pInput, new PipedOutputStream(pInput), 10000000, 1000, 1000);





        System.out.println("checkConsistency buffer size 1");

        input = new NonBlockingPipedInputStream(1);

        NonBlockingPipedInputStream.checkConsistency(input, input.outputStream(), 1000, 100, 100);


        System.out.println("checkConsistency default buffer size");

        input = new NonBlockingPipedInputStream();

        NonBlockingPipedInputStream.checkConsistency(input, input.outputStream(), 10000000, 1000, 1000);


        System.out.println("checkConsistency buffer size 1mb");

        input = new NonBlockingPipedInputStream(1048576);

        NonBlockingPipedInputStream.checkConsistency(input, input.outputStream(), 10000000, 1000, 1000);





    }


    private static void speedTest(final InputStream input, final OutputStream out, final int iterations, final int bytesPerWrite, final int bytesPerRead) throws Throwable {

        long elapsed = NonBlockingPipedInputStream.speedTest(input, out, iterations, bytesPerWrite, bytesPerRead);

        System.out.println("Speed test of " + input.getClass() + ": " + (((double) elapsed) / 1000000.0) + "ms (" + elapsed + " ns)");
    }

}
