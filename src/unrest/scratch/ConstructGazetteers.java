package unrest.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import unrest.util.UnrestProperties;

import ark.util.FileUtil;

public class ConstructGazetteers {
	private static UnrestProperties properties = new UnrestProperties(false);
	
	public static void main(String[] args) throws IOException {
		constructUnrestTermLarge();
		constructCity();
		constructCountry();
		constructCityCountryMap();
		constructLocationLanguageMap();
	}

	private static void constructLocationLanguageMap() throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Gazetteer/Source/locationLanguages_fromDavid.txt");
		
		String line = null;
		Map<String, Set<String>> languagesToLocations = new HashMap<String, Set<String>>();
		while ((line = r.readLine()) != null) {
			String[] lineParts = line.split("\t");
			String location = lineParts[0].trim();
			String language = lineParts[1].trim();
			
			if (!languagesToLocations.containsKey(language))
				languagesToLocations.put(language, new HashSet<String>());
			languagesToLocations.get(language).add(location);
		}
		r.close();
		
		writeGazetteer(languagesToLocations, properties.getLocationLanguageMapGazetteerPath());
	}

	private static void constructCityCountryMap() throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Gazetteer/Source/city_country_map_fromDavid.txt");
		
		String line = null;
		Map<String, Set<String>> countryToCities = new HashMap<String, Set<String>>();
		while ((line = r.readLine()) != null) {
			String[] lineParts = line.split("\t");
			if (lineParts.length < 2)
				continue;
			
			String city = lineParts[0];
			String country = lineParts[1];
			
			if (!countryToCities.containsKey(country))
				countryToCities.put(country, new HashSet<String>());
			countryToCities.get(country).add(city);
		}
		
		r.close();
		
		writeGazetteer(countryToCities, properties.getCityCountryMapGazetteerPath());
	}

	private static void constructCountry() throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Gazetteer/Source/countries_fromDavid.txt");

		Map<String, Set<String>> countries = new HashMap<String, Set<String>>();
		String line = null;
		while ((line = r.readLine()) != null) {
			String[] lineParts = line.split("\t");
			String country = lineParts[lineParts.length - 1];
			if (!countries.containsKey(country))
				countries.put(country, new HashSet<String>());
			
			for (int i = 1; i < lineParts.length - 1; i++)
				countries.get(country).add(lineParts[i]);
		}
		r.close();
		writeGazetteer(countries, properties.getCountryGazetteerPath());
	}

	private static void constructCity() throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Gazetteer/Source/cities_fromDavid.txt");
		Map<String, Set<String>> cities = new HashMap<String, Set<String>>();
		String line = null;
		while ((line = r.readLine()) != null) {
			String[] lineParts = line.split("\t");
			String city = lineParts[lineParts.length - 1];
			if (!cities.containsKey(city))
				cities.put(city, new HashSet<String>());
			
			for (int i = 1; i < lineParts.length - 1; i++)
				cities.get(city).add(lineParts[i]);
		}
		
		r.close();
		writeGazetteer(cities, properties.getCityGazetteerPath());
	}

	private static void constructUnrestTermLarge() throws IOException {
		BufferedReader r = FileUtil.getFileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Gazetteer/Source/allqueryterms_fromDavid.txt");

		String line = null;
		Map<String, Set<String>> terms = new HashMap<String, Set<String>>();
		int id = 0;
		while ((line = r.readLine()) != null) {
			String[] lineParts = line.split("\t");
			
			terms.put(String.valueOf(id), new HashSet<String>());
			terms.get(String.valueOf(id)).add(lineParts[0].trim());
			
			id++;
		}
		r.close();
		writeGazetteer(terms, properties.getUnrestTermLargeGazetteerPath());
	}
	
	private static void writeGazetteer(Map<String, Set<String>> gazetteerValues, String path) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(path));
		
		for (Entry<String, Set<String>> entry : gazetteerValues.entrySet()) {
			w.write(entry.getKey() + "\t");
			for (String location : entry.getValue())
				w.write(location + "\t");
			w.write("\n");
		}
		
		w.close();
	}
}
