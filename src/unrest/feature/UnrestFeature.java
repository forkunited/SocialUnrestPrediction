package unrest.feature;

import java.util.Calendar;
import java.util.Map;

public abstract class UnrestFeature {
	public abstract String getName();
	public abstract Map<String, Integer> compute(String text, Calendar textTime, String location);
	public abstract Map<String, Integer> preCompute(String text, Calendar textTime, String location);
	
	public Map<String, Integer> compute(String text) {
		return compute(text, null, null);
	}
}
