package unrest.facebook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ark.util.FileUtil;

import unrest.util.UnrestProperties;

public class AggregateTermMap {
	public class Aggregates {
		private double mean;
		private double sd;
		private int count;
		
		public Aggregates(double mean, double sd, int count) {
			this.mean = mean;
			this.sd = sd;
			this.count = count;
		}
		
		public double getMean() {
			return this.mean;
		}
		
		public double getSD() {
			return this.sd;
		}
		
		public int getCount() {
			return this.count;
		}
		
		public String toString() {
			return this.mean + "\t" + this.sd + "\t" + this.count;
		}
	}
	
	private String language;
	private Map<String, Map<String, Aggregates>> aggregateMap;
	
	public AggregateTermMap(String language) {
		this(language, true);
	}
	
	public AggregateTermMap(String language, boolean loadFromFile) {
		this.language = language;
		this.aggregateMap = new HashMap<String, Map<String, Aggregates>>();
		if (loadFromFile)
			load();
	}
	
	public boolean hasFeature(String featureType, String featureTerm) {
		return this.aggregateMap.containsKey(featureType) && this.aggregateMap.get(featureType).containsKey(featureTerm);
	}
	
	public double getMean(String featureType, String featureTerm) {
		if (!this.aggregateMap.containsKey(featureType) || !this.aggregateMap.get(featureType).containsKey(featureTerm))
			return -1.0;
		return this.aggregateMap.get(featureType).get(featureTerm).getMean();
	}
	
	public double getSD(String featureType, String featureTerm) {
		if (!this.aggregateMap.containsKey(featureType) || !this.aggregateMap.get(featureType).containsKey(featureTerm))
			return -1.0;
		
		return this.aggregateMap.get(featureType).get(featureTerm).getSD();
	}
	
	public int getCount(String featureType, String featureTerm) {
		if (!this.aggregateMap.containsKey(featureType) || !this.aggregateMap.get(featureType).containsKey(featureTerm))
			return -1;
		
		return this.aggregateMap.get(featureType).get(featureTerm).getCount();
	}
	
	public Map<String, Map<String, Integer>> getVocabulary(int limit) {
		Map<String, Map<String, Integer>> vocabulary = new HashMap<String, Map<String, Integer>>();
		
		for (Entry<String, Map<String, Aggregates>> featureEntry : this.aggregateMap.entrySet()) {			
			List<Entry<String, Aggregates>> termList = new ArrayList<Entry<String, Aggregates>>(featureEntry.getValue().entrySet());
			Collections.sort(termList, 
				new Comparator<Entry<String, Aggregates>>() {
            		public int compare(Entry<String, Aggregates> o1, Entry<String, Aggregates> o2) {
            			if (o1.getValue().getCount() > o2.getValue().getCount()) {
            				return -1;
            			} else if (o1.getValue().getCount() < o2.getValue().getCount()) {
            				return 1;
            			} else {
            				return 0;
            			}
            		}
		        }
			);
			
			Map<String, Integer> topTerms = new HashMap<String, Integer>();
			for (int i = 0; i < Math.min(limit, termList.size()); i++) {
				topTerms.put(termList.get(i).getKey(), termList.get(i).getValue().getCount());
			}
			
			vocabulary.put(featureEntry.getKey(), topTerms);
		}
		
		return vocabulary;
	}
	
	public void addAggregates(String featureType, String featureTerm, double mean, double sd, int count) {
		if (!this.aggregateMap.containsKey(featureType))
			this.aggregateMap.put(featureType, new HashMap<String, Aggregates>());
		this.aggregateMap.get(featureType).put(featureTerm, new Aggregates(mean, sd, count));
	}
	
	public void save(Map<String, Map<String, Integer>> vocabFilter, String pathPrefix) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(pathPrefix + "." + this.language));
			
			for (Entry<String, Map<String, Aggregates>> featureEntry : this.aggregateMap.entrySet()) {
				if (!vocabFilter.containsKey(featureEntry.getKey()))
					continue;
				for (Entry<String, Aggregates> termEntry : featureEntry.getValue().entrySet()) {
					if (!vocabFilter.get(featureEntry.getKey()).containsKey(termEntry.getKey()))
						continue;
					bw.write(featureEntry.getKey() + "\t" + termEntry.getKey() + "\t" + termEntry.getValue().toString() + "\n");
				}
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void load() {
		try {
			UnrestProperties properties = new UnrestProperties();
			BufferedReader br = FileUtil.getFileReader(properties.getFacebookFeatureAggregatePathPrefix() + "." + this.language);
			
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String featureType = lineParts[0];
				String featureTerm = lineParts[1];
				double mean = Double.parseDouble(lineParts[2]);
				double sd = Double.parseDouble(lineParts[3]);
				int count = Integer.parseInt(lineParts[4]);
				
				if (!this.aggregateMap.containsKey(featureType))
					this.aggregateMap.put(featureType, new HashMap<String, Aggregates>());
				this.aggregateMap.get(featureType).put(featureTerm, new Aggregates(mean, sd, count));
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
