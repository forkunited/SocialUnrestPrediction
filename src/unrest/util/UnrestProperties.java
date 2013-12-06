package unrest.util;

import ark.util.ARKProperties;

public class UnrestProperties extends ARKProperties {
	/* Gazetteers From BBN for unrest.detector */
	private String unrestTermGazetteerPath;
	private String unrestLocationGazetteerPath;
	
	/* Gazetteers from previous Twitter logistic regression */
	private String unrestTermLargeGazetteerPath;
	private String cityGazetteerPath;
	private String countryGazetteerPath;
	private String cityCountryMapGazetteerPath;
	private String locationLanguageMapGazetteerPath;
	
	/* Files for training unrest model on Facebook data */
	private String facebookPostDateLocationTotalsPath;
	private String facebookFeatureVocabPathPrefix;
	private String facebookFeatureAggregatePathPrefix;
	
	private String facebookAppID;
	private String facebookAppSecret;
	private String facebookDataScrapeDirPath;
	
	private int maxThreads;
	
	public UnrestProperties() {
		super(new String[] { "unrest.properties", "/user/wmcdowell/osi/Projects/SocialUnrestPrediction/unrest.properties"});
			
		this.unrestTermGazetteerPath = loadProperty("unrestTermGazetteerPath");
		this.unrestLocationGazetteerPath = loadProperty("unrestLocationGazetteerPath");
		
		this.unrestTermLargeGazetteerPath = loadProperty("unrestTermLargeGazetteerPath");
		this.cityGazetteerPath = loadProperty("cityGazetteerPath");
		this.countryGazetteerPath = loadProperty("countryGazetteerPath");
		this.cityCountryMapGazetteerPath = loadProperty("cityCountryMapGazetteerPath");
		this.locationLanguageMapGazetteerPath = loadProperty("locationLanguageMapGazetteerPath");
		
		this.facebookPostDateLocationTotalsPath = loadProperty("facebookPostDateLocationTotalsPath");
		this.facebookFeatureVocabPathPrefix = loadProperty("facebookFeatureVocabPathPrefix");
		this.facebookFeatureAggregatePathPrefix = loadProperty("facebookFeatureAggregatePathPrefix");
		
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
	
	public String getUnrestTermLargeGazetteerPath() {
		return this.unrestTermLargeGazetteerPath;
	}
	
	public String getCityGazetteerPath() {
		return this.cityGazetteerPath;
	}
	
	public String getCountryGazetteerPath() {
		return this.countryGazetteerPath;
	}
	
	public String getCityCountryMapGazetteerPath() {
		return this.cityCountryMapGazetteerPath;
	}
		
	public String getLocationLanguageMapGazetteerPath() {
		return this.locationLanguageMapGazetteerPath;
	}
	
	public String getFacebookPostDateLocationTotalsPath() {
		return this.facebookPostDateLocationTotalsPath;
	}
	
	public String getFacebookFeatureVocabPathPrefix() {
		return this.facebookFeatureVocabPathPrefix;
	}
	
	public String getFacebookFeatureAggregatePathPrefix() {
		return this.facebookFeatureAggregatePathPrefix;
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
