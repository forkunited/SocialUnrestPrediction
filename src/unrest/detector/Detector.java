package unrest.detector;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import net.sf.json.JSONObject;

public abstract class Detector {
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	// This will probably change later
	public class Prediction {
		private Calendar minTime;
		private Calendar maxTime;
		private String location;
		private String sourceText;
		
		public Prediction(Calendar minTime, Calendar maxTime, String location, String sourceText) {
			this.minTime = minTime;
			this.maxTime = maxTime;
			this.location = location;
			this.sourceText = sourceText;
		}
		
		public Calendar getMinTime() {
			return this.minTime;
		}
		
		public Calendar getMaxTime() {
			return this.maxTime;
		}
		
		public String getLocation() {
			return this.location;
		}
		
		public void setMinTime(Calendar minTime) {
			this.minTime = minTime;
		}
		
		public void setMaxTime(Calendar maxTime) {
			this.maxTime = maxTime;
		}
		
		public void setLocation(String location) {
			this.location = location;
		}
		
		@Override
		public String toString() {
			JSONObject json = new JSONObject();
			json.put("text", this.sourceText);
			json.put("minTime", dateFormat.format(this.minTime.getTime()));
			json.put("maxTime", dateFormat.format(this.maxTime.getTime()));
			json.put("location", this.location);
			return json.toString();
		}
	}
	
	public Prediction getPrediction(String text) {
		return getPrediction(text, null);
	}
	
	public abstract Prediction getPrediction(String text, Calendar textTime);
}
