package unrest.detector;

import java.util.Calendar;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DetectorFutureDate extends Detector {
	private static Pattern datePattern = Pattern.compile("(\\d+) de ((?:enero)|(?:febrero)|(?:marzo)|(?:abril)|(?:mayo)|(?:junio)|(?:julio)|(?:agosto)|(?:septiembre)|(?:octubre)|(?:noviembre)|(?:diciembre))");
	private static Pattern dayPattern = Pattern.compile("(?:lunes)|(?:martes)|(?:miércoles)|(?:jueves)|(?:viernes)|(?:sábado)|(?:domingo)");

	private static Map<String, Integer> monthsTable = new TreeMap<String, Integer>();
	static {
		monthsTable.put("enero", Calendar.JANUARY);
		monthsTable.put("febrero", Calendar.FEBRUARY);
		monthsTable.put("marzo", Calendar.MARCH);
		monthsTable.put("abril", Calendar.APRIL);
		monthsTable.put("mayo", Calendar.MAY);
		monthsTable.put("junio", Calendar.JUNE);
		monthsTable.put("julio", Calendar.JULY);
		monthsTable.put("agosto", Calendar.AUGUST);
		monthsTable.put("septiembre", Calendar.SEPTEMBER);
		monthsTable.put("octubre", Calendar.OCTOBER);
		monthsTable.put("noviembre", Calendar.NOVEMBER);
		monthsTable.put("diciembre", Calendar.DECEMBER);
	}

	private static TreeMap<String, Integer> dayTable = new TreeMap<String, Integer>();
	static {
		dayTable.put("lunes", Calendar.MONDAY);
		dayTable.put("martes", Calendar.TUESDAY);
		dayTable.put("miércoles", Calendar.WEDNESDAY);
		dayTable.put("jueves", Calendar.THURSDAY);
		dayTable.put("viernes", Calendar.FRIDAY);
		dayTable.put("sábado", Calendar.SATURDAY);
		dayTable.put("domingo", Calendar.SUNDAY);
	}
	
	public DetectorFutureDate() {
		
	}
	
	@Override
	public Prediction getPrediction(String text, Calendar textTime) {
		Matcher dateMatcher = datePattern.matcher(text);
		if (dateMatcher.find()) {
			int day = Integer.parseInt(dateMatcher.group(1));
			int month = monthsTable.get(dateMatcher.group(2));	
			if (month == textTime.get(Calendar.MONTH)) {
				if (day > textTime.get(Calendar.DATE)) {
					Calendar newCal = (Calendar)textTime.clone();
					newCal.set(Calendar.DATE, day);
					return new Prediction(newCal, newCal, null, text);
				}
			} else if (month == textTime.get(Calendar.MONTH) + 1) {
				Calendar newCal = (Calendar) textTime.clone();
				newCal.set(textTime.get(Calendar.YEAR), month, day);
				return new Prediction(newCal, newCal, null, text);
			} else if ((month == textTime.get(Calendar.JANUARY)) && (textTime.get(Calendar.MONTH) == Calendar.JANUARY)) {
				Calendar newCal = (Calendar) textTime.clone();
				newCal.set(textTime.get(Calendar.YEAR) + 1, month, day);
				return new Prediction(newCal, newCal, null, text);
			}
		}
		
		Matcher dayMatcher = dayPattern.matcher(text);
		if (dayMatcher.find()) {
			int day = dayTable.get(dayMatcher.group());
			int dayDiff = day - textTime.get(Calendar.DAY_OF_WEEK);
			if (dayDiff != 0) {
				if (dayDiff < 0) {
					dayDiff += 7;
				}
				
				Calendar newCal = (Calendar) textTime.clone();
				newCal.add(Calendar.DATE, dayDiff);
				return new Prediction(newCal, newCal, null, text);
			}
		} else if (text.contains("mañana")) {
			Calendar newCal = (Calendar) textTime.clone();
			newCal.add(Calendar.DATE, 1);
			return new Prediction(newCal, newCal, null, text);
		}
		
		return null;
	}
	
}
