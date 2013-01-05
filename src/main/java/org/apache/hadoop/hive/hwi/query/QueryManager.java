package org.apache.hadoop.hive.hwi.query;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MQuery;

public class QueryManager {

    protected static final Log l4j = LogFactory.getLog(QueryManager.class
            .getName());

    private static QueryManager instance;

    private ThreadPoolExecutor executor;
    private QueryMonitor monitor;

    private QueryManager() {
    }

    public static QueryManager getInstance() {
        if (instance == null) {
            synchronized (QueryManager.class) {
                if (instance == null) {
                    instance = new QueryManager();
                }
            }
        }
        return instance;
    }

    public void start() {
        startExecutor();
        startMonitor();
        l4j.info("QueryManager created.");
    }

    protected void startExecutor() {
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
        l4j.info("Executor started.");
    }

    protected void startMonitor() {
        monitor = new QueryMonitor();
        monitor.start();
        l4j.info("Monitor started.");
    }

    public boolean submit(MQuery mquery) {
        if (executor == null)
            return false;

        QueryWorker worker = new QueryWorker(mquery);

        executor.execute(worker);

        if (monitor != null)
            monitor.monitor(worker);

        return true;
    }

    public void shutdown() {
        if (executor != null)
            executor.shutdownNow();
        if (monitor != null)
            monitor.shutdown();
        l4j.info("QueryManager shutdown.");
    }
}
