package unrest.detector;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import ark.data.Gazetteer;

public class DetectorGazetteer extends Detector {
	private Set<String> gazetteerValues;
	private Calendar minDate;
	private Calendar maxDate;
	private boolean predictLocation;
	
	public DetectorGazetteer() {
		this(new TreeSet<String>(), false);
	}
	
	public DetectorGazetteer(Gazetteer gazetteer, boolean predictLocation) {
		this(gazetteer.getValues(), predictLocation);
	}
	
	public DetectorGazetteer(Set<String> gazetteerValues, boolean predictLocation) {
		this.gazetteerValues = gazetteerValues;
		
		this.maxDate = Calendar.getInstance();
		this.maxDate.setTime(new Date(Long.MAX_VALUE));
		this.minDate = Calendar.getInstance();
		this.minDate.setTime(new Date(Long.MIN_VALUE));
		this.predictLocation = predictLocation;
	}
	
	public void setGazetteerValues(Set<String> gazetteerValues) {
		this.gazetteerValues = gazetteerValues;
	}
	
	public void setGazetteerValues(String value) {
		this.gazetteerValues.clear();
		this.gazetteerValues.add(value);
	}
	
	@Override
	public Prediction getPrediction(String text, Calendar textTime) {
		String[] textParts = text.split("\\s+");
		for (String gazetteerValue : this.gazetteerValues) {
			String[] gazetteerValueParts = gazetteerValue.split("\\s+");
			for (int i = 0; i < textParts.length - (gazetteerValueParts.length - 1); i++) {
				boolean matches = true;
				for (int j = 0; j < gazetteerValueParts.length; j++) {
					if (!textParts[i+j].equals(gazetteerValueParts[j])) {
						matches = false;
						break;
					}
				}
				
				if (matches) {
					return new Prediction(this.minDate, this.maxDate, (this.predictLocation) ? gazetteerValue : null, text);
				}
			}	
		}
		
		return null;
	}
}
