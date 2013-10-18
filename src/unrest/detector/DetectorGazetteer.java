package unrest.detector;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import unrest.data.Gazetteer;

public class DetectorGazetteer extends Detector {
	private Set<String> gazetteerValues;
	private Calendar minDate;
	private Calendar maxDate;
	private boolean predictLocation;
	
	public DetectorGazetteer(Gazetteer gazetteer, boolean predictLocation) {
		this.gazetteerValues = gazetteer.getValues();
		
		this.maxDate = Calendar.getInstance();
		this.maxDate.setTime(new Date(Long.MAX_VALUE));
		this.minDate = Calendar.getInstance();
		this.minDate.setTime(new Date(Long.MIN_VALUE));
		this.predictLocation = predictLocation;
	}
	
	@Override
	public Prediction getPrediction(String text, Calendar textTime) {
		for (String gazetteerValue : this.gazetteerValues) {
			if (text.contains(gazetteerValue))
				return new Prediction(this.minDate, this.maxDate, (this.predictLocation) ? gazetteerValue : null, text);
		}
		
		return null;
	}
}
