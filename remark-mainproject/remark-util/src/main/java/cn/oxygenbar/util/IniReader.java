package cn.oxygenbar.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;

public class IniReader {
	protected HashMap<String, Properties> sections = new HashMap<String, Properties>();
	private transient String currentSecion;
	private transient Properties current;

	private static IniReader instance = null;
	private static String iniFileName = "";

	public static IniReader getInstance() {
		return getInstance(0);
	}

	public static IniReader getinstance_extra() {
		return getInstance(1);
	}

	public static IniReader getInstance(int type) {
		if (type == 0) {
			iniFileName = "remark.ini";
		}
		if (instance == null) {
			syncInit();
		}
		return instance;
	}

	private static synchronized void syncInit() {
		if (instance == null) {
			try {
				instance = new IniReader();
			} catch (Exception e) {
				Logs.geterrorLogger().error(e.getMessage(), e);
			}
		}
	}

	public IniReader() throws IOException {
		InputStream stream = getClass().getClassLoader().getResourceAsStream(
				iniFileName);
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(stream));
		read(reader);
		reader.close();
	}

	protected void read(BufferedReader reader) throws IOException {
		String line;
		while ((line = reader.readLine()) != null) {
			parseLine(line);
			sections.put(currentSecion, current);
		}
	}

	protected void parseLine(String line) {
		line = line.trim();
		if (line.matches("\\[.*\\]")) {
			if (current != null) {
				sections.put(currentSecion, current);
			}
			currentSecion = line.replaceFirst("\\[(.*)\\]", "$1");
			current = new Properties();
			sections.put(currentSecion, current);
		} else if (line.matches(".*=.*")) {
			if (current != null) {
				int i = line.indexOf('=');
				String name = line.substring(0, i);
				String value = line.substring(i + 1);
				current.setProperty(name, value);
			}
		}
	}

	public String getValue(String section, String name) {
		Properties p = (Properties) sections.get(section);

		if (p == null) {
			return null;
		}

		String value = p.getProperty(name);
		return value;
	}

	public Properties getSection(String section) {
		return (Properties) sections.get(section);
	}

	public static void main(String[] args) throws IOException {
		System.out.println(IniReader.getInstance(0).getValue("onlinesys",
				"ifonline"));

	}

}
