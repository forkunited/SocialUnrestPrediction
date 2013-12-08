package unrest.util;

import java.util.List;

import ark.data.Gazetteer;

public class LocationLanguageMap {
	private UnrestProperties properties;
	private Gazetteer cityCountryMapGazetteer;
	private Gazetteer locationLanguageMapGazetteer;
	
	public LocationLanguageMap() {
		this(new UnrestProperties());
	}
	
	public LocationLanguageMap(UnrestProperties properties) {
		this.properties = properties;
		this.cityCountryMapGazetteer =  new Gazetteer("CityCountryMap", this.properties.getCityCountryMapGazetteerPath());
		this.locationLanguageMapGazetteer = new Gazetteer("LocationLanguageMap", this.properties.getLocationLanguageMapGazetteerPath());
	}
	
	public String getLanguage(String location) {
		List<String> langs = this.locationLanguageMapGazetteer.getIds(location);
		if (langs != null && !langs.isEmpty())
			return langs.get(0);
		
		List<String> countries = this.cityCountryMapGazetteer.getIds(location);
		if (countries == null)
			return null;
		for (String country : countries) {
			langs = this.locationLanguageMapGazetteer.getIds(country);
			if (langs != null && !langs.isEmpty())
				return langs.get(0);
		}
		
		return null;
	}
}
