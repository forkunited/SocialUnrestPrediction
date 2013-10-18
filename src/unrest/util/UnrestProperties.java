package unrest.util;

import java.io.FileReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class UnrestProperties {
	private String unrestTermGazetteerPath;
	private String unrestLocationGazetteerPath;
	
	public UnrestProperties(String propertiesPath) {
		try {
			FileReader reader = new FileReader(propertiesPath);
			Properties properties = new Properties();
			properties.load(reader);
			Map<String, String> env = System.getenv();
			
			this.unrestTermGazetteerPath = loadProperty(env, properties, "unrestTermGazetteerPath");
			this.unrestLocationGazetteerPath = loadProperty(env, properties, "unrestLocationGazetteerPath");
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String loadProperty(Map<String, String> env, Properties properties, String property) {
		String propertyValue = properties.getProperty(property);
		for (Entry<String, String> envEntry : env.entrySet())
			propertyValue = propertyValue.replace("${" + envEntry.getKey() + "}", envEntry.getValue());
		return propertyValue;
	}
	
	public String getUnrestTermGazetteerPath() {
		return this.unrestTermGazetteerPath;
	}
	
	public String getUnrestLocationGazetteerPath() {
		return this.unrestLocationGazetteerPath;
	}
}
