/**
 * Copyright (C) [2013] [Anjuke Inc]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
