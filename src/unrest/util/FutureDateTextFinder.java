package unrest.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.util.Pair;

public class FutureDateTextFinder {
	
	private static Pattern tomorrowPattern = Pattern.compile("(?:ma.ana)|(?:amanh.)|(?:tomorrow)");
	
	private static Pattern datePattern;
	private static Map<String, Integer> months = new TreeMap<String, Integer>();
	static {
		/* Spanish */
		months.put("enero", Calendar.JANUARY);
		months.put("febrero", Calendar.FEBRUARY);
		months.put("marzo", Calendar.MARCH);
		months.put("abril", Calendar.APRIL);
		months.put("mayo", Calendar.MAY);
		months.put("junio", Calendar.JUNE);
		months.put("julio", Calendar.JULY);
		months.put("agosto", Calendar.AUGUST);
		months.put("septiembre", Calendar.SEPTEMBER);
		months.put("octubre", Calendar.OCTOBER);
		months.put("noviembre", Calendar.NOVEMBER);
		months.put("diciembre", Calendar.DECEMBER);
		
		/* Portugese */
		months.put("janeiro", Calendar.JANUARY);
		months.put("fevereiro", Calendar.FEBRUARY);
		months.put("mar.o", Calendar.MARCH);
		months.put("abril", Calendar.APRIL);
		months.put("maio", Calendar.MAY);
		months.put("junho", Calendar.JUNE);
		months.put("julho", Calendar.JULY);
		months.put("agosto", Calendar.AUGUST);
		months.put("setembro", Calendar.SEPTEMBER);
		months.put("outubro", Calendar.OCTOBER);
		months.put("novembro", Calendar.NOVEMBER);
		months.put("dezembro", Calendar.DECEMBER);
	
		StringBuilder datePatternStr = new StringBuilder();
		datePatternStr.append("(\\d+) de (");
		for (Entry<String, Integer> entry : months.entrySet())
			datePatternStr.append("(?:").append(entry.getKey()).append(")|");
		datePatternStr.delete(datePatternStr.length()-1, datePatternStr.length());
		datePatternStr.append(")");
		datePattern = Pattern.compile(datePatternStr.toString());
	}
	
	private static Pattern dayPattern;
	private static Map<String, Integer> days = new TreeMap<String, Integer>();
	static {
		/* Spanish */
		days.put("lunes", Calendar.MONDAY);
		days.put("martes", Calendar.TUESDAY);
		days.put("mi.rcoles", Calendar.WEDNESDAY);
		days.put("jueves", Calendar.THURSDAY);
		days.put("viernes", Calendar.FRIDAY);
		days.put("s.bado", Calendar.SATURDAY);
		days.put("domingo", Calendar.SUNDAY);
		
		/* Portugese */
		days.put("segundafeira", Calendar.MONDAY);
		days.put("ter.afeira", Calendar.TUESDAY);
		days.put("quartafeira", Calendar.WEDNESDAY);
		days.put("quintafeira", Calendar.THURSDAY);
		days.put("sextafeira", Calendar.FRIDAY);
		days.put("sextafeira", Calendar.SATURDAY);
		days.put("s.bado", Calendar.SUNDAY);

		StringBuilder dayPatternStr = new StringBuilder();
		for (Entry<String, Integer> entry : days.entrySet())
			dayPatternStr.append("(?:").append(entry.getKey()).append(")|");
		dayPatternStr.delete(dayPatternStr.length()-1, dayPatternStr.length());
		dayPattern = Pattern.compile(dayPatternStr.toString());
	}

	private Calendar currentDate;
	
	public FutureDateTextFinder(Calendar currentDate) {
		this.currentDate = currentDate;
	}
	
	public FutureDateTextFinder() {
		this.currentDate = Calendar.getInstance();
	}
	
	public void setCurrentDate(Calendar currentDate) {
		this.currentDate = currentDate;
	}
	
	public List<Calendar> findFutureDates(String text) {
		List<Calendar> futureDates = new ArrayList<Calendar>();
		Calendar futureDate = null;
		int futureDateIndex = 0;
		do {
			futureDate = null;
			Pair<Calendar, Integer> nextDate = nextDate(text);
			Pair<Calendar, Integer> nextDay = nextDay(text);
			Pair<Calendar, Integer> nextTomorrow = nextTomorrow(text);
			
			if (nextDate != null && 
					(nextDay == null || nextDate.second() <= nextDay.second()) && 
					(nextTomorrow == null || nextDate.second() <= nextTomorrow.second())) {
				futureDate = nextDate.first();
				futureDateIndex = nextDate.second();
			} else if (nextDay != null && 
						(nextDate == null || nextDay.second() <= nextDate.second()) && 
						(nextTomorrow == null || nextDay.second() <= nextTomorrow.second())) {
				futureDate = nextDay.first();
				futureDateIndex = nextDay.second();
			} else if (nextTomorrow != null) {
				futureDate = nextTomorrow.first();
				futureDateIndex = nextTomorrow.second();
			}
			
			if (futureDate != null) {
				futureDates.add(futureDate);
				text = text.substring(futureDateIndex);
			}
		} while (futureDate != null);
		
		return futureDates;
	}

	private Pair<Calendar, Integer> nextDate(String text) {
		Matcher dateMatcher = datePattern.matcher(text);
		Calendar nextDate = null;
		if (dateMatcher.find()) {
			int day = 0;
			try {
				day = Integer.parseInt(dateMatcher.group(1));
			} catch (NumberFormatException e) {
				return null; // Happens sometimes if the number is too big
			}
			
			int month = getMonthIndex(dateMatcher.group(2));	
			if (month == this.currentDate.get(Calendar.MONTH)) {
				if (day > this.currentDate.get(Calendar.DATE)) {
					nextDate = (Calendar)this.currentDate.clone();
					nextDate.set(Calendar.DATE, day);
					return new Pair<Calendar, Integer>(nextDate, dateMatcher.end());
				}
			} else if (month == this.currentDate.get(Calendar.MONTH) + 1) {
				nextDate = (Calendar)this.currentDate.clone();
				nextDate.set(this.currentDate.get(Calendar.YEAR), month, day);
				return new Pair<Calendar, Integer>(nextDate, dateMatcher.end());
			} else if ((month == this.currentDate.get(Calendar.JANUARY)) && (this.currentDate.get(Calendar.MONTH) == Calendar.JANUARY)) {
				nextDate = (Calendar)this.currentDate.clone();
				nextDate.set(this.currentDate.get(Calendar.YEAR) + 1, month, day);
				return new Pair<Calendar, Integer>(nextDate, dateMatcher.end());
			}
		}
		
		return null;
	}
	
	private Pair<Calendar, Integer> nextDay(String text) {
		Matcher dayMatcher = dayPattern.matcher(text);

		if (dayMatcher.find()) {
			int day = getDayIndex(dayMatcher.group());
			int dayDiff = day - this.currentDate.get(Calendar.DAY_OF_WEEK);
			if (dayDiff != 0) {
				if (dayDiff < 0) {
					dayDiff += 7;
				}
				
				Calendar nextDay = (Calendar)this.currentDate.clone();
				nextDay.add(Calendar.DATE, dayDiff);
				return new Pair<Calendar, Integer>(nextDay, dayMatcher.end());
			}
		}
		
		return null;
	}
	
	private Pair<Calendar, Integer> nextTomorrow(String text) {
		Matcher tomorrowMatcher = tomorrowPattern.matcher(text);
		if (tomorrowMatcher.find()) {
			Calendar nextTomorrow = (Calendar) this.currentDate.clone();
			nextTomorrow.add(Calendar.DAY_OF_MONTH, 1);
			return new Pair<Calendar, Integer>(nextTomorrow, tomorrowMatcher.end());
		}
		
		return null;
	}
	
	private int getMonthIndex(String monthStr) {
		for (Entry<String, Integer> entry : months.entrySet()) {
			if (monthStr.matches(entry.getKey()))
					return entry.getValue();
		}
		return -1;
	}
	
	private int getDayIndex(String dayStr) {
		for (Entry<String, Integer> entry : days.entrySet()) {
			if (dayStr.matches(entry.getKey()))
					return entry.getValue();
		}
		return -1;	
	}
}
