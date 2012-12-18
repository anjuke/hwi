package org.apache.hadoop.hive.hwi;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TestEverything {

  /**
   * @param args
   * @throws Exception
   * @throws IOException
   */
  public static void main(String[] args) throws Exception{
    //testCli(args);
    //testHistoryFile();
  }

  private static void testHistoryFile() throws IOException {
    String s = "1\r2\n3";
    System.out.println(s.replace('\r', ' ').replace('\n', ' '));
  }

  private static String readFile(String path) throws IOException{
    BufferedReader br = new BufferedReader(new FileReader(path));
    StringBuffer str = new StringBuffer();
    String line = br.readLine();
    while (line != null)
    {
        str.append(line);
        str.append("\n");
        line = br.readLine();
    }

    return str.toString();
  }

}
