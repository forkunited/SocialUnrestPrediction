package unrest.feature;

import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;

public class UnrestFeatureFixedEffects extends UnrestFeature {
	
	public UnrestFeatureFixedEffects() {
		
	}

	@Override
	public String getName() {
		return "fixedEffects";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime, String location) {
		Map<String, Integer> values = new TreeMap<String, Integer>();

		// Add effects:
		// Day of week
		// Location x Month
		values.put("dayOfWeek_" + textTime.get(Calendar.DAY_OF_WEEK), 1);
		values.put("locationMonth_" + location + "_" + textTime.get(Calendar.MONTH) + "_" + textTime.get(Calendar.YEAR), 1);
		
		return values;
	}
}
