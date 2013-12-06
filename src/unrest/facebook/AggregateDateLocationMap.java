package unrest.facebook;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AggregateDateLocationMap {
	private Map<String, Map<String, Double>> aggregateMap;
	private int aggregateCount;
	
	public AggregateDateLocationMap(String sourceFilePath) {
		this.aggregateMap = new HashMap<String, Map<String, Double>>();
		this.aggregateCount = 0;
		load(sourceFilePath);
	}
	
	public double getAggregate(String date, String location) {
		return this.aggregateMap.get(date).get(location);
	}
	
	public int getAggregateCount() {
		return this.aggregateCount;
	}
	
	private void load(String sourceFilePath) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(sourceFilePath));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String date = lineParts[0];
				String location = lineParts[1];
				double aggregate = Double.parseDouble(lineParts[2]);
				if (!this.aggregateMap.containsKey(date))
					this.aggregateMap.put(date, new HashMap<String, Double>());
				this.aggregateMap.get(date).put(location, aggregate);
				this.aggregateCount++;
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
