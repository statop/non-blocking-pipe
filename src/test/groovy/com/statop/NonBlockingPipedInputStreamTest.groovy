package com.statop;

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors



class NonBlockingPipedInputStreamTest extends GroovyTestCase {

    public void testPipe() {

        Random r = new Random(System.nanoTime());

        NonBlockingPipedInputStream[] nbInputs = [
            new NonBlockingPipedInputStream(1),
            new NonBlockingPipedInputStream(r.nextInt(20000) + 1),
            new NonBlockingPipedInputStream(r.nextInt(20000) + 1),
            new NonBlockingPipedInputStream(r.nextInt(20000) + 1),
            new NonBlockingPipedInputStream(r.nextInt(20000) + 1),
            new NonBlockingPipedInputStream(r.nextInt(20000) + 1)
        ]

        PipedInputStream[] pipeInputs = new PipedInputStream[nbInputs.length];

        NonBlockingPipedOutputStream[] nbOutputs = new NonBlockingPipedOutputStream[nbInputs.length];
        PipedOutputStream[] pipeOutputs = new PipedOutputStream[nbInputs.length];

        ExecutorService[] nbWriterThreads = new ExecutorService[nbInputs.length];
        ExecutorService[] pipeWriterThreads = new ExecutorService[nbInputs.length];

        for (int x = 0; x < nbInputs.length; ++x) {
            pipeInputs[x] = new PipedInputStream(nbInputs[x].getBufferSize());
            nbOutputs[x] = nbInputs[x].outputStream();
            pipeOutputs[x] = new PipedOutputStream(pipeInputs[x]);
            nbWriterThreads[x] = Executors.newSingleThreadExecutor();
            pipeWriterThreads[x] = Executors.newSingleThreadExecutor();
        }


        Thread writeThread = new Thread({
            Random rand = new Random(System.nanoTime());

            for (int y = 0; y < 1000; ++y) {
                for (int k = 0; k < nbInputs.length; ++k) {
                    int x = k;
                    if (rand.nextBoolean()) {
                        byte[] buf = new byte[rand.nextInt(1000) + 1];
                        rand.nextBytes(buf);
                        if (rand.nextBoolean()) {
                            nbWriterThreads[x].execute({nbOutputs[x].write(buf)} as Runnable);
                            pipeWriterThreads[x].execute({pipeOutputs[x].write(buf)} as Runnable);
                        } else {
                            int off = rand.nextInt(buf.length);
                            int len = rand.nextInt(buf.length - off)
                            nbWriterThreads[x].execute({nbOutputs[x].write(buf, off, len)} as Runnable);
                            pipeWriterThreads[x].execute({pipeOutputs[x].write(buf, off, len)} as Runnable);
                        }
                    } else {
                        int i = rand.nextInt();
                        nbWriterThreads[x].execute({nbOutputs[x].write(i)} as Runnable);
                        pipeWriterThreads[x].execute({pipeOutputs[x].write(i)} as Runnable);
                    }
                }
            }

            for (int k = 0; k < nbInputs.length; ++k) {
                int x = k;
                nbWriterThreads[x].execute({nbOutputs[x].close()} as Runnable);
                pipeWriterThreads[x].execute({pipeOutputs[x].close()} as Runnable);
            }

        } as Runnable, "TestNonBlockingPipedInputStreamThread");
        writeThread.setDaemon(true)

        writeThread.start();

        long iteration = 0;
        while(true) {
            boolean keepGoing = false;
            for (int x = 0; x < nbInputs.length; ++x) {
                boolean done = false;
                InputStream a;
                InputStream b;

                if (r.nextBoolean()) {
                    a = pipeInputs[x];
                    b = nbInputs[x];
                } else {
                    a = nbInputs[x];
                    b = pipeInputs[x];
                }

                if (r.nextBoolean()) {
                    byte[] buf1 = new byte[r.nextInt(1000) + 1];
                    byte[] buf2 = new byte[buf1.length];

                    int requestedLen;
                    int read;
                    int off;

                    if (r.nextBoolean()) {
                        requestedLen = buf1.length;
                        read = a.read(buf1);
                        off = 0;
                    } else {
                        off = r.nextInt(buf2.length);
                        requestedLen = r.nextInt(buf2.length - off);
                        read = a.read(buf1, off, requestedLen)
                    }

                    if (read < 0) {
                        done = true;
                        assertEquals(read, b.read(buf2, off, requestedLen))
                    } else {
                        int read2 = 0;
                        while (read2 < read) {
                            int k = b.read(buf2, off + read2, read - read2);
                            assertTrue(k >= 0);
                            read2 += k;
                        }
                    }

                    assertEquals(Arrays.toString(buf1), Arrays.toString(buf2));
                } else {
                    int k = a.read();
                    if (k < 0) {
                        done = true;
                    }
                    assertEquals(k, b.read());
                }
                if (!done) {
                    keepGoing = true;
                }
            }
            if (!keepGoing) {
                break;
            }
            ++iteration;
        }


//        return;


        println "speed test buffer size 1"

        long seeda = System.nanoTime();
        long seedb = System.currentTimeMillis();

        NonBlockingPipedInputStream input = new NonBlockingPipedInputStream(1);

        speedTest(input, input.outputStream(), new Random(seeda), new Random(seedb), 10000);

        PipedInputStream pInput = new PipedInputStream(1);

        speedTest(pInput, new PipedOutputStream(pInput), new Random(seeda), new Random(seedb), 10000);



        println "speed test default buffer size"

        input = new NonBlockingPipedInputStream();

        speedTest(input, input.outputStream(), new Random(seeda), new Random(seedb), 100000000);

        pInput = new PipedInputStream(input.getBufferSize());

        speedTest(pInput, new PipedOutputStream(pInput), new Random(seeda), new Random(seedb), 100000000);




        println "speed test buffer size 1mb"

        input = new NonBlockingPipedInputStream(1048576);

        speedTest(input, input.outputStream(), new Random(seeda), new Random(seedb), 100000000);

        pInput = new PipedInputStream(1048576);

        speedTest(pInput, new PipedOutputStream(pInput), new Random(seeda), new Random(seedb), 100000000);

    }


    private static void speedTest(final InputStream input, final OutputStream out, final Random ra, final Random rb, final int iterations) {

        long elapsed = NonBlockingPipedInputStream.speedTest(input, out, ra, rb, iterations);

        println ("Speed test of ${input.class}: ${((double) elapsed) / 1000000.0} ms ($elapsed ns)")
    }

}
