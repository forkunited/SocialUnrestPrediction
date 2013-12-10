package unrest.scratch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class EvaluateDetectedUnrestFacebook {
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
	
	public static void main(String[] args) {
		String predictedUnrestFilter = args[0];
		String actualUnrestPath = args[1];
		String predictedUnrestPath = args[2];
		String minDate = null;
		String maxDate = null;
		
		if (args.length > 3) {
			minDate = args[3];
			maxDate = args[4];
		} 
		
		Set<String> predictedUnrest = readUnrestFile(predictedUnrestPath, minDate, maxDate, predictedUnrestFilter);
		Set<String> actualUnrest = readUnrestFile(actualUnrestPath, minDate, maxDate, null);
		
		double correctCount = 0;
		for (String predicted : predictedUnrest) {
			if (actualUnrest.contains(predicted))
				correctCount += 1.0;
		}
		
		double p = correctCount/predictedUnrest.size(); // precision
		double r = correctCount/actualUnrest.size(); // recall
		double f1 = (2.0*p*r)/(p+r);
		
		System.out.println("Precision: " + p);
		System.out.println("Recall: " + r);
		System.out.println("F1: " + f1);
	}
	
	private static Set<String> readUnrestFile(String path, String minDateStr, String maxDateStr, String filter) {
		Set<String> unrestLocationDates = new HashSet<String>();
		Calendar minDate = null;
		Calendar maxDate = null;
		if (minDateStr != null && maxDateStr != null) {
			minDate = Calendar.getInstance();
			maxDate = Calendar.getInstance();
			
			try {
				minDate.setTime(dateFormat.parse(minDateStr));
				maxDate.setTime(dateFormat.parse(maxDateStr));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String location = null;
				String dateStr = null;
				if (filter != null) {
					if (!lineParts[0].equals(filter))
						continue;
					location = lineParts[1];
					dateStr = lineParts[2];
				} else {
					location = lineParts[0];
					dateStr = lineParts[1];
				}
				
				Calendar date = Calendar.getInstance();
				try {
					date.setTime(dateFormat.parse(dateStr));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				
				if (minDate != null && maxDate != null && (date.compareTo(minDate) < 0 || date.compareTo(maxDate) > 0))
					continue;
				
				unrestLocationDates.add(location + "\t" + dateStr);
			}
			
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return unrestLocationDates;
	}
}
