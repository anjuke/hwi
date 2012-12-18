package org.apache.hadoop.hive.hwi;

import org.apache.hadoop.hive.shims.JettyShims;
import org.apache.hadoop.hive.shims.ShimLoader;

public class TestRunHWIServer {

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    JettyShims.Server webServer;
    webServer = ShimLoader.getJettyShims().startServer("0.0.0.0", 9999);

    webServer.addWar("src/main/webapp/", "/hwi");

    webServer.start();
    webServer.join();

  }

}
