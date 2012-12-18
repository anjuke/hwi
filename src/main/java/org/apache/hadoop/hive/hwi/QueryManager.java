package org.apache.hadoop.hive.hwi;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MQuery;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;

public class QueryManager implements Runnable {

  protected static final Log l4j = LogFactory.getLog(QueryManager.class
      .getName());

  private final ThreadPoolExecutor executor;
  private boolean goOn;
  private final LinkedBlockingQueue<QueryWorker> workers;

  protected QueryManager() {
    goOn = true;
    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
    workers = new LinkedBlockingQueue<QueryWorker>();
  }

  public void run() {
    while (goOn) {
      try {
        //blocking if no more worker
        QueryWorker worker = workers.take();

        Status status = worker.getStatus();
        switch (status) {
        case INITED:
          l4j.debug("find inited worker");
          workers.put(worker);
          break;
        case RUNNING:
          worker.running();
          workers.put(worker);
          break;
        default:
          l4j.debug("remove worker:" + status);
          break;
        }

        l4j.debug("go to sleep...");
        Thread.sleep(100);
      } catch (InterruptedException e) {
        l4j.error(e.getMessage());
      }
    }

    l4j.debug("goOn is false. Loop has ended.");

    executor.shutdown();
  }

  public boolean submit(MQuery mquery) {
    if (!goOn) {
      return false;
    }

    QueryWorker worker = new QueryWorker(mquery);
    executor.execute(worker);

    //nonblocking
    workers.offer(worker);

    return true;
  }

  protected boolean isGoOn() {
    return goOn;
  }

  protected void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }

}
