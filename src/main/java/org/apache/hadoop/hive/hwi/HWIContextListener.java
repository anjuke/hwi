/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hwi;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * After getting a contextInitialized event this component starts an instance of
 * the HiveSessionManager.
 *
 */
public class HWIContextListener implements javax.servlet.ServletContextListener {

  protected static final Log l4j = LogFactory.getLog(HWIContextListener.class
      .getName());

  /**
   * The Hive Web Interface manages multiple hive sessions. This event is used
   * to start a Runnable, QueryManager as a thread inside the servlet
   * container.
   *
   * @param sce
   *          An event fired by the servlet context on startup
   */
  public void contextInitialized(ServletContextEvent sce) {
    ServletContext sc = sce.getServletContext();

    QueryManager qm = new QueryManager();
    l4j.debug("QueryManager created.");
    Thread t = new Thread(qm);
    t.start();
    l4j.debug("QueryManager thread started.");
    sc.setAttribute("qm", qm);

    l4j.debug("QueryManager placed in application context.");
  }

  /**
   * When the Hive Web Interface is closing we locate the Runnable
   * HiveSessionManager and set it's internal goOn variable to false. This
   * should allow the application to gracefully shutdown.
   *
   * @param sce
   *          An event fired by the servlet context on context shutdown
   */
  public void contextDestroyed(ServletContextEvent sce) {
    ServletContext sc = sce.getServletContext();
    QueryManager qm = (QueryManager) sc.getAttribute("qm");

    if (qm == null) {
      l4j.error("QueryManager was not found in context");
    } else {
      l4j.error("QueryManager goOn set to false. Shutting down.");
      qm.setGoOn(false);
    }

  }
}
