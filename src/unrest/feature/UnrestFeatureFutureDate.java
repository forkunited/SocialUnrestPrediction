package unrest.feature;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import unrest.util.FutureDateTextFinder;
import ark.util.StringUtil;

public class UnrestFeatureFutureDate extends UnrestFeature {
	private StringUtil.StringTransform cleanFn;
	private boolean onlyTomorrow;
	private FutureDateTextFinder finder;
	private Calendar textTimePlusOne;
	
	public UnrestFeatureFutureDate(boolean onlyTomorrow) {
		this.cleanFn = StringUtil.getDefaultCleanFn();
		this.onlyTomorrow = onlyTomorrow;
		this.finder = new FutureDateTextFinder();
		this.textTimePlusOne = Calendar.getInstance();
	}

	@Override
	public String getName() {
		return "futureDate";
	}

	@Override
	public Map<String, Integer> compute(String text, Calendar textTime, String location) {
		Map<String, Integer> values = new TreeMap<String, Integer>();
		String cleanText = this.cleanFn.transform(text);
		
		this.finder.setCurrentDate(textTime);
		
		List<Calendar> futureDates = this.finder.findFutureDates(cleanText);
		
		if (this.onlyTomorrow) {
			this.textTimePlusOne.setTime(textTime.getTime());
			this.textTimePlusOne.add(Calendar.DAY_OF_MONTH, 1);
			
			for (Calendar futureDate : futureDates) {
				if (futureDate.get(Calendar.DAY_OF_YEAR) == this.textTimePlusOne.get(Calendar.DAY_OF_YEAR)
						&& futureDate.get(Calendar.YEAR) == this.textTimePlusOne.get(Calendar.YEAR)) {
					values.put("future", 1);
					break;
				}
			}
		} else if (futureDates.size() > 0) {
			values.put("future", 1);
		}
		
		return values;
	}
	
	@Override
	public Map<String, Integer> preCompute(String text, Calendar textTime,
			String location) {
		// TODO Auto-generated method stub
		return null;
	}
}
