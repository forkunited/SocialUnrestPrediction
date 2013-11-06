package unrest.util;

import ark.util.ARKProperties;

public class UnrestProperties extends ARKProperties {
	private String unrestTermGazetteerPath;
	private String unrestLocationGazetteerPath;
	private String facebookAppID;
	private String facebookAppSecret;
	private String facebookDataScrapeDirPath;
	private int maxThreads;
	
	public UnrestProperties() {
		super(new String[] { "unrest.properties", "/user/wmcdowell/osi/Projects/SocialUnrestPrediction/unrest.properties"});
			
		this.unrestTermGazetteerPath = loadProperty("unrestTermGazetteerPath");
		this.unrestLocationGazetteerPath = loadProperty("unrestLocationGazetteerPath");
		this.facebookAppID = loadProperty("facebookAppID");
		this.facebookAppSecret = loadProperty("facebookAppSecret");
		this.facebookDataScrapeDirPath = loadProperty("facebookDataScrapeDirPath");
		this.maxThreads = Integer.parseInt(loadProperty("maxThreads"));
			
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
	
	public int getMaxThreads() {
		return this.maxThreads;
	}
}
