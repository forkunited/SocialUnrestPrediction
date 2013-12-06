package unrest.feature;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import unrest.detector.DetectorGazetteer;

import ark.data.Gazetteer;
import ark.util.StringUtil;

public class UnrestFeatureGazetteer extends UnrestFeature {
	private DetectorGazetteer detector;
	private Set<String> gazetteerValues;
	private StringUtil.StringTransform cleanFn;
	
	public UnrestFeatureGazetteer(Gazetteer gazetteer) {
		this.detector = new DetectorGazetteer();
		this.gazetteerValues = gazetteer.getValues();
		this.cleanFn = StringUtil.getDefaultCleanFn();
	}

	@Override
	public String getName() {
		return "hand";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime) {
		Map<String, Integer> values = new HashMap<String, Integer>(this.gazetteerValues.size());
		String cleanText = this.cleanFn.transform(text);
		for (String gazetteerValue : this.gazetteerValues) {
			this.detector.setGazetteerValues(gazetteerValue);
			if (this.detector.getPrediction(cleanText) != null)
				values.put(gazetteerValue.trim().replaceAll("\\s+", "_"), 1);
		}
		
		return values;
	}
}
