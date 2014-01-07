package unrest.facebook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import ark.util.FileUtil;

import net.sf.json.JSONObject;

/**
 * Takes in lines output from HFeaturizeFacebookPostSReg of the form:
 * 
 * [Date].[Location]	[Sentence ID i]	{"[f_i0]":[v_i1], "[f_i1]":[v_i1],...}
 * 
 * And outputs lines of the form:
 * 
 * [Date].[Location]	{"[Sentence ID 0]":{"[f_00]":[v_00], "[f_01]":[v_01],...}, "[Sentence ID 1]":{"[f_10]":[v_10], "[f_11]":[v_11],...}...}
 * 
 * The output can be fed into Dani's sentence regularizing model.
 * 
 */
public class AggregateSRegFeatures {
	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		try {
			BufferedReader br = FileUtil.getFileReader(inputPath);
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputPath));
			String line = null;
			String curDateLocation = null;
			JSONObject curObj = new JSONObject();
			Set<String> dateLocations = new HashSet<String>();
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.split("\\t");
				String dateLocation = lineParts[0];
				String sentenceId = lineParts[1];
				String featureObjStr = lineParts[2];
				
				if (curDateLocation == null || !dateLocation.equals(curDateLocation)) {
					if (curDateLocation != null)
						bw.write(curDateLocation + "\t" + curObj.toString() + "\n");
					
					curDateLocation = dateLocation;
					curObj = new JSONObject();
					
					if (dateLocations.contains(curDateLocation)) {
						System.out.println("ERROR: Duplicate date/location " + curDateLocation);
						System.exit(0);
					}
					System.out.println("Aggregating for date/location: " + curDateLocation);
					dateLocations.add(curDateLocation);
				}
				
				curObj.put(sentenceId, featureObjStr);
			}
			
			if (curObj.size() > 0)
				bw.write(curDateLocation + "\t" + curObj.toString() + "\n");
			
			br.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
}
