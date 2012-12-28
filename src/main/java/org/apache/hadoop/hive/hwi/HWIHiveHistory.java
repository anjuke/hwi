package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * HiveHistory.parseLine has concurrent issue! 
 * In HiveHistory.parseLine, the static member parseBuffer was used to store parse result, which is stupid
 * What is worse, parseLine is private, so I CAN'T simply overide it!
 * AND I dont' know why there are so many FUCKING private static final members!
 **/
public class HWIHiveHistory extends HiveHistory {

	public static final String KEY = "(\\w+)";
	public static final String VALUE = "[[^\"]?]+"; // anything but a " in ""
	public static final String ROW_COUNT_PATTERN = "TABLE_ID_(\\d+)_ROWCOUNT";

	public static final Pattern pattern = Pattern.compile(KEY + "=" + "\""
			+ VALUE + "\"");

	public HWIHiveHistory(SessionState ss) {
		super(ss);
	}

	public static void parseHiveHistory(String path, Listener l)
			throws IOException {
		FileInputStream fi = new FileInputStream(path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fi));
		try {
			String line = null;
			StringBuilder buf = new StringBuilder();
			while ((line = reader.readLine()) != null) {
				buf.append(line);
				// if it does not end with " then it is line continuation
				if (!line.trim().endsWith("\"")) {
					continue;
				}
				parseLine(buf.toString(), l);
				buf = new StringBuilder();
			}
		} finally {
			try {
				reader.close();
			} catch (IOException ex) {
			}
		}
	}

	protected static void parseLine(String line, Listener l) throws IOException {
		// extract the record type
		int idx = line.indexOf(' ');
		String recType = line.substring(0, idx);
		String data = line.substring(idx + 1, line.length());

		Matcher matcher = pattern.matcher(data);

		Map<String, String> parseBuffer = new HashMap<String, String>();

		while (matcher.find()) {
			String tuple = matcher.group(0);
			String[] parts = tuple.split("=");

			parseBuffer.put(parts[0],
					parts[1].substring(1, parts[1].length() - 1));
		}

		l.handle(RecordTypes.valueOf(recType), parseBuffer);
	}

}
