package unrest.util;

import java.io.File;

import ark.util.ARKProperties;

public class UnrestProperties extends ARKProperties {
	public static String PROPERTIES_PATH = "unrest.properties";
	
	/* Path to Gazetteer directory */
	private String gazetteerDirPath;

	/* Paths to input, temporary, and output files of Facebook data featurizer */
	private String facebookFileNamePrefix;
	private String facebookOutputDirPath;
	
	/* Facebook scraper configuration */
	private String facebookAppID;
	private String facebookAppSecret;
	private String facebookDataScrapeDirPath;
	
	private int maxThreads;
	
	public UnrestProperties(boolean useHdfs) {
		// FIXME: Use environment variable for these 
		super(new String[] { PROPERTIES_PATH });
		
		this.gazetteerDirPath = (useHdfs) ? loadProperty("gazetteerHdfsDirPath") : loadProperty("gazetteerLocalDirPath");
		
		this.facebookFileNamePrefix = loadProperty("facebookFileNamePrefix");
		this.facebookOutputDirPath = (useHdfs) ? loadProperty("facebookHdfsOutputDirPath") : loadProperty("facebookLocalOutputDirPath");
		
		this.facebookAppID = loadProperty("facebookAppID");
		this.facebookAppSecret = loadProperty("facebookAppSecret");
		this.facebookDataScrapeDirPath = loadProperty("facebookDataScrapeDirPath");
		
		this.maxThreads = Integer.parseInt(loadProperty("maxThreads"));	
	}
	
	public String getUnrestTermGazetteerPath() {
		return (new File(this.gazetteerDirPath, "UnrestTerm.gazetteer")).getAbsolutePath();
	}
	
	public String getUnrestLocationGazetteerPath() {
		return (new File(this.gazetteerDirPath, "UnrestLocation.gazetteer")).getAbsolutePath();
	}
	
	public String getUnrestTermLargeGazetteerPath() {
		return (new File(this.gazetteerDirPath, "UnrestTermLarge.gazetteer")).getAbsolutePath();
	}
	
	public String getCityGazetteerPath() {
		return (new File(this.gazetteerDirPath, "City.gazetteer")).getAbsolutePath();
	}
	
	public String getCountryGazetteerPath() {
		return (new File(this.gazetteerDirPath, "Country.gazetteer")).getAbsolutePath();
	}
	
	public String getCityCountryMapGazetteerPath() {
		return (new File(this.gazetteerDirPath, "CityCountry.gazetteer")).getAbsolutePath();
	}
		
	public String getLocationLanguageMapGazetteerPath() {
		return (new File(this.gazetteerDirPath, "LocationLanguageMap.gazetteer")).getAbsolutePath();
	}
	
	public String getFacebookPostDateLocationTotalsPath() {
		return (new File(this.facebookOutputDirPath, this.facebookFileNamePrefix + "DateLocationPostCounts")).getAbsolutePath();
	}
	
	public String getFacebookFeatureVocabPathPrefix() {
		return (new File(this.facebookOutputDirPath, this.facebookFileNamePrefix + "TermAggregates.vocab")).getAbsolutePath();
	}
	
	public String getFacebookFeatureAggregatePathPrefix() {
		return (new File(this.facebookOutputDirPath, this.facebookFileNamePrefix + "TermAggregates.agg")).getAbsolutePath();
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
