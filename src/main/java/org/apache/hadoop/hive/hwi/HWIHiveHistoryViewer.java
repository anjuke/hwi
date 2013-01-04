package org.apache.hadoop.hive.hwi;

import java.io.IOException;

import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;

public class HWIHiveHistoryViewer extends HiveHistoryViewer {

	public HWIHiveHistoryViewer(String historyFile) {
		super(historyFile);
		try {
			HWIHiveHistory.parseHiveHistory(historyFile, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
