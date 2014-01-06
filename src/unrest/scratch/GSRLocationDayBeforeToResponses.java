package unrest.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ark.util.FileUtil;

public class GSRLocationDayBeforeToResponses {
	private static String startDateStr = "01-01-2013";
	private static String endDateStr = "05-31-2013";
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
	
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		try {
			Map<String, Set<String>> locationsToDates = loadGSRLocationDayBefore(inputPath);
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath));
			for (Entry<String, Set<String>> entry : locationsToDates.entrySet()) {
				String location = entry.getKey();
				Set<String> positiveDates = entry.getValue();
				
				Date date = dateFormat.parse(startDateStr);
				Date endDate = dateFormat.parse(endDateStr);
                Calendar cal = Calendar.getInstance();
                
                while(date.getTime() <= endDate.getTime()) {
                    String dateStr = dateFormat.format(date);
                    String responseKey = dateStr + "." + location;
                    
                    int responseValue = 0;
                    if (positiveDates.contains(dateStr))
                    	responseValue = 1;
                    
                    bw.write(responseKey + "\t" + responseValue + "\n");
                	
                	cal.setTime(date);
                    cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) + 1);
                    date = cal.getTime();
                }
			}
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static Map<String, Set<String>> loadGSRLocationDayBefore(String path) throws IOException {
		BufferedReader br = FileUtil.getFileReader(path);
		Map<String, Set<String>> locationsToDates = new HashMap<String, Set<String>>();
		
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] lineParts = line.split("\t");
			String location = lineParts[0];
			String date = lineParts[1];
			
			if (!locationsToDates.containsKey(location))
				locationsToDates.put(location, new HashSet<String>());
			locationsToDates.get(location).add(date);
		}
		
		br.close();
		return locationsToDates;
	}
}
