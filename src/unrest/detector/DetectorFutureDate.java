package unrest.detector;

import java.util.Calendar;
import java.util.List;

import unrest.util.FutureDateTextFinder;

public class DetectorFutureDate extends Detector {
	private FutureDateTextFinder finder;
	
	public DetectorFutureDate() {
		this.finder = new FutureDateTextFinder();
	}
	
	@Override
	public Prediction getPrediction(String text, Calendar textTime) {
		this.finder.setCurrentDate(textTime);
		List<Calendar> futureDates = this.finder.findFutureDates(text);
		if (futureDates.size() > 0) {
			return new Prediction(futureDates.get(0), futureDates.get(0), null, text);
		}
		
		return null;
	}
	
}
