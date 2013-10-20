package unrest.util;

import java.io.FileReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class UnrestProperties {
	private String unrestTermGazetteerPath;
	private String unrestLocationGazetteerPath;
	private String facebookAppID;
	private String facebookAppSecret;
	private String facebookDataScrapeDirPath;
	
	public UnrestProperties(String propertiesPath) {
		try {
			FileReader reader = new FileReader(propertiesPath);
			Properties properties = new Properties();
			properties.load(reader);
			Map<String, String> env = System.getenv();
			
			this.unrestTermGazetteerPath = loadProperty(env, properties, "unrestTermGazetteerPath");
			this.unrestLocationGazetteerPath = loadProperty(env, properties, "unrestLocationGazetteerPath");
			this.facebookAppID = loadProperty(env, properties, "facebookAppID");
			this.facebookAppSecret = loadProperty(env, properties, "facebookAppSecret");
			this.facebookDataScrapeDirPath = loadProperty(env, properties, "facebookDataScrapeDirPath");
			
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
	
	public String getFacebookAppID() {
		return this.facebookAppID;
	}
	
	public String getFacebookAppSecret() {
		return this.facebookAppSecret;
	}
	
	public String getFacebookDataScrapeDirPath() {
		return this.facebookDataScrapeDirPath;
	}
}
