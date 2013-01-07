package org.apache.hadoop.hive.hwi.query;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;

public class QueryMonitor implements Runnable {
    protected static final Log l4j = LogFactory.getLog(QueryMonitor.class
            .getName());

    private LinkedBlockingQueue<QueryRunner> runners;

    private Thread t;

    protected QueryMonitor() {
        runners = new LinkedBlockingQueue<QueryRunner>();
    }

    public void start() {
        t = new Thread(this);
        t.start();
    }

    public void run() {
        l4j.info("QueryMonitor started.");

        while (true) {
            try {
                // blocking if no more runner
                QueryRunner runner = runners.take();

                Status status = runner.getStatus();
                switch (status) {
                case INITED:
                    l4j.debug("find inited runner");
                    runners.put(runner);
                    break;
                case RUNNING:
                    runner.running();
                    runners.put(runner);
                    break;
                default:
                    l4j.debug("remove runner:" + status);
                    break;
                }

                l4j.debug("go to sleep...");
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
        }

    }

    public boolean monitor(QueryRunner runner) {
        return runners.offer(runner);
    }

    public void shutdown() {
        l4j.info("QueryMonitor shutting down.");
        try {
            t.interrupt();
            t.join(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        l4j.info("QueryMonitor shutdown complete.");
    }
}
