package unrest.feature;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import ark.util.StringUtil;

public class UnrestFeatureUnigram extends UnrestFeature {
	private StringUtil.StringTransform cleanFn;
	
	public UnrestFeatureUnigram() {
		this.cleanFn = StringUtil.getDefaultCleanFn();
	}

	@Override
	public String getName() {
		return "unigram";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime) {
		Map<String, Integer> values = new HashMap<String, Integer>();
		String[] tokens = this.cleanFn.transform(text).split("\\s+");
		
		for (int i = 0; i < tokens.length; i++) {
			values.put(tokens[i], 1);
		}
		
		return values;
	}
}
