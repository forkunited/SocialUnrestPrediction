package unrest.detector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class DetectorConjunction extends Detector {
	protected List<Detector> detectors;
	
	public DetectorConjunction() {
		this.detectors = new ArrayList<Detector>();
	}
	
	public DetectorConjunction(List<Detector> detectors) {
		this.detectors = detectors;
	}
	
	@Override
	public Prediction getPrediction(String text, Calendar textTime) {
		Prediction prediction = null;
		for (Detector detector : this.detectors) {
			Prediction currentPrediction = detector.getPrediction(text, textTime);
			if (currentPrediction == null)
				return null;
			
			if (prediction == null) {
				prediction = new Prediction(currentPrediction.getMinTime(), currentPrediction.getMaxTime(), currentPrediction.getLocation(), text);
				continue;
			}
			
			if (prediction.getLocation() != null && currentPrediction.getLocation() != null && !prediction.getLocation().equals(currentPrediction.getLocation()))
				return null;
			else if (prediction.getLocation() == null)
				prediction.setLocation(currentPrediction.getLocation());
				
			if (currentPrediction.getMinTime().compareTo(prediction.getMinTime()) > 0)
				prediction.setMinTime(currentPrediction.getMinTime());
			if (currentPrediction.getMaxTime().compareTo(prediction.getMaxTime()) < 0)
				prediction.setMaxTime(currentPrediction.getMaxTime());
			
			if (prediction.getMinTime().compareTo(prediction.getMaxTime()) > 0)
				return null;
		}
		return prediction;
	}
	
}
