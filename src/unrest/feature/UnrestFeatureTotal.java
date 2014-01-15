package unrest.feature;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class UnrestFeatureTotal extends UnrestFeature {
	public UnrestFeatureTotal() {
		
	}
	
	@Override
	public String getName() {
		return "total";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime, String location) {
		Map<String, Integer> values = new HashMap<String, Integer>(1);
		values.put("total", 1);
		return values;
	}
	
	@Override
	public Map<String, Integer> preCompute(String text, Calendar textTime,
			String location) {
		// TODO Auto-generated method stub
		return null;
	}

}
