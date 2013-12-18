package unrest.facebook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import unrest.util.LocationLanguageMap;

/**
 * Takes in a document output from HConstructTrainingData containing lines of the form:
 * 
 * [Feature Type]	[Date]	[Location]	[Feature Value List]
 * 
 * And outputs a files [Prefix].[Language].[Feature Type] containing lines of the form:
 * 
 * [Location]	[Date]	[Feature Value List]
 * 
 */
public class TrainingDataSplitter {
	public static void main(String[] args) {
		String trainingDataInputPath = args[0];
		String outputPathPrefix = args[1];
		
		Set<String> validFeatures = new TreeSet<String>();
		validFeatures.add("hand");
		validFeatures.add("unigram");
		validFeatures.add("handTom");
		validFeatures.add("unigramTom");
		
		LocationLanguageMap languageMap = new LocationLanguageMap(false);
		Map<String, Map<String, List<String>>> languageToFeatureToLines = new HashMap<String, Map<String, List<String>>>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(trainingDataInputPath));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String feature = lineParts[0];
				String date = lineParts[1];
				String location = lineParts[2];
				String featureValueList = lineParts[3];
				String language = languageMap.getLanguage(location);
			
				if (!validFeatures.contains(feature))
					continue;
				
				if (!languageToFeatureToLines.containsKey(language))
					languageToFeatureToLines.put(language, new HashMap<String, List<String>>());
				if (!languageToFeatureToLines.get(language).containsKey(feature))
					languageToFeatureToLines.get(language).put(feature, new ArrayList<String>());
				languageToFeatureToLines.get(language).get(feature).add(location + "\t" + date + "\t" + featureValueList);
			}
			
			br.close();
		
			for (Entry<String, Map<String, List<String>>> languageEntry : languageToFeatureToLines.entrySet()) {
				for (Entry<String, List<String>> featureEntry : languageEntry.getValue().entrySet()) {
					BufferedWriter bw = new BufferedWriter(new FileWriter(outputPathPrefix + "." + languageEntry.getKey() + "." + featureEntry.getKey()));
				
					for (String outputLine : featureEntry.getValue()) {
						bw.write(outputLine + "\n");
					}
					
					bw.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
}
