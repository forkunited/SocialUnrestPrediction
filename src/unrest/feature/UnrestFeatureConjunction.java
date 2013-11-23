package unrest.feature;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class UnrestFeatureConjunction extends UnrestFeature {
	private String name;
	private UnrestFeature feature1;
	private UnrestFeature feature2;
	
	public UnrestFeatureConjunction(String name, UnrestFeature feature1, UnrestFeature feature2) {
		this.name = name;
		this.feature1 = feature1;
		this.feature2 = feature2;
	}
	
	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime) {
		Map<String, Integer> values = new HashMap<String, Integer>();
		Map<String, Integer> feature1Values = this.feature1.compute(text);
		Map<String, Integer> feature2Values = this.feature2.compute(text);
		
		// TODO: For now, this just uses first feature's keys, but make more flexible
		// later if necessary
		for (Entry<String, Integer> feature1Entry : feature1Values.entrySet()) {
			for (Entry<String, Integer> feature2Entry : feature2Values.entrySet()) {
				values.put(feature1Entry.getKey(), feature1Entry.getValue()*feature2Entry.getValue());
			}
		}
		
		return values;
	}

}
