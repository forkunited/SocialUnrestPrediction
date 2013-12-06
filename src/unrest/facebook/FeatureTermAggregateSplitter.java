package unrest.facebook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Takes in a document containing lines of the form:
 * 
 * [Language]	[Feature Type]	[Feature Term]	[Mean]	[Standard Deviation]	[Count]
 * 
 * And outputs a files [Language].means.sd containing lines of the form:
 * 
 * [Feature Type]	[Feature Term]	[Mean]	[Standard Deviation]
 * 
 * And also files [Language].[Feature Type].vocabulary containing lines of the form:
 * 
 * [Count]	[Feature Term]
 * 
 * The output files contain only data from input lines with the top N values for [Count] with
 * N=25000 by default.
 * 
 */
public class FeatureTermAggregateSplitter {
	public static void main(String[] args) {
		String aggregateTermInputPath = args[0];
		String featureVocabOutputPathPrefix = args[1];
		String featureAggregateOutputPathPrefix = args[2];
		
		int N = 25000;
		if (args.length > 1)
			N = Integer.parseInt(args[3]);
		
		Map<String, AggregateTermMap> termMaps = new HashMap<String, AggregateTermMap>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(aggregateTermInputPath));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String language = lineParts[0];
				String featureType = lineParts[1];
				String featureTerm = lineParts[2];
				double mean = Double.parseDouble(lineParts[3]);
				double sd = Double.parseDouble(lineParts[4]);
				int count = Integer.parseInt(lineParts[5]);
				
				if (!termMaps.containsKey(language))
					termMaps.put(language, new AggregateTermMap(language, false));
				termMaps.get(language).addAggregates(featureType, featureTerm, mean, sd, count);
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		for (Entry<String, AggregateTermMap> languageEntry : termMaps.entrySet()) {
			Map<String, Map<String, Integer>> vocabulary = languageEntry.getValue().getVocabulary(N);
			
			// Save term aggregates for language
			languageEntry.getValue().save(vocabulary, featureAggregateOutputPathPrefix);
			
			for (Entry<String, Map<String, Integer>> featureEntry : vocabulary.entrySet()) {
				try {
					BufferedWriter bw = new BufferedWriter(new FileWriter(featureVocabOutputPathPrefix + "." + languageEntry.getKey() + "." + featureEntry.getKey()));
				
					for (Entry<String, Integer> termEntry : featureEntry.getValue().entrySet()) {
						bw.write(termEntry.getValue() + "\t" + termEntry.getKey() + "\n");
					}
					
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
			}
		}
	}
}
