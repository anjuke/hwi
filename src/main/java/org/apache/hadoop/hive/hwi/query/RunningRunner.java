package org.apache.hadoop.hive.hwi.query;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RunningRunner implements Runnable {
    protected static final Log l4j = LogFactory.getLog(RunningRunner.class
            .getName());

    public static enum Progress {
        CONTINUE, EXIT
    };

    public static interface Running {
        public Progress running();
    }

    private LinkedBlockingQueue<Running> runnings;

    private Thread t;

    public RunningRunner() {
        runnings = new LinkedBlockingQueue<Running>();
    }

    public void start() {
        t = new Thread(this);
        t.start();
    }

    public void run() {
        l4j.info("RunningRunner started.");

        while (true) {
            try {
                // block if no more running
                Running running = runnings.take();

                Progress progress = running.running();
                switch (progress) {
                case CONTINUE:
                    runnings.put(running);
                    break;
                case EXIT:
                    break;
                }

                l4j.debug("go to sleep...");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }

    }

    public boolean add(Running running) {
        return runnings.offer(running);
    }

    public void shutdown() {
        l4j.info("RunningRunner shutting down.");
        try {
            t.interrupt();
            t.join(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        l4j.info("RunningRunner shutdown complete.");
    }
}
