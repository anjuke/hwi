package org.apache.hadoop.hive.hwi.query;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hwi.model.MQuery.Status;

public class QueryMonitor implements Runnable {
	protected static final Log l4j = LogFactory.getLog(QueryMonitor.class
			.getName());

	private boolean goOn;
	private LinkedBlockingQueue<QueryWorker> workers;

	protected QueryMonitor() {
		goOn = false;
		workers = new LinkedBlockingQueue<QueryWorker>();
	}

	public void start(){
		goOn = true;
		Thread t = new Thread(this);
	    t.start();
	}
	
	public void run() {
	    l4j.info("QueryMonitor started.");
		while (goOn) {
			try {
				// blocking if no more worker
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

		l4j.info("QueryMonitor stopped.");
	}

	public boolean monitor(QueryWorker worker) {
		return workers.offer(worker);
	}

	public void shutdown() {
		this.goOn = false;
	}
}
