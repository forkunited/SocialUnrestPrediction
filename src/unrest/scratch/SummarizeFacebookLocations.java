package unrest.scratch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import unrest.util.LocationLanguageMap;
import unrest.util.UnrestProperties;

import com.restfb.json.JsonArray;
import com.restfb.json.JsonObject;

public class SummarizeFacebookLocations {
	public static void main(String[] args) {
		UnrestProperties properties = new UnrestProperties(false);
		LocationLanguageMap languageMap = new LocationLanguageMap(properties);
		
		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		SimpleDateFormat argumentDateFormat = new SimpleDateFormat("MM-dd-yyyy");
		Calendar date = Calendar.getInstance();
		Calendar minDate = Calendar.getInstance();
		Calendar maxDate = Calendar.getInstance();
		
		try {
			minDate.setTime(argumentDateFormat.parse(args[0]));
			maxDate.setTime(argumentDateFormat.parse(args[1]));
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		File inputDir = new File(properties.getFacebookDataScrapeDirPath(), "PageData");
		File[] dataFiles = inputDir.listFiles();
		int totalPosts = 0;
		int timePosts = 0;
		int countryTimePosts = 0;
		int spanishTimePosts = 0;
		int cityTimePosts = 0;
		for (File dataFile : dataFiles) {
			List<JsonObject> data = readData(dataFile);
			if (data == null)
				continue;
			if (!dataFile.getName().startsWith("facebookPageData_"))
				continue;
			int dataFileId = Integer.parseInt(dataFile.getName().substring(17, dataFile.getName().length()));
			
			for (JsonObject datum : data) {
				String type = datum.getString("type");
				JsonObject response = datum.getJsonObject("response");
				
				if (!type.equals("FEED"))
					continue;
				
				JsonArray posts = response.getJsonArray("data");
				for (int i = 0; i < posts.length(); i++) {
					JsonObject post = posts.getJsonObject(i);
					if (!post.has("message"))
						continue;
					
					totalPosts++;
					
					if (!post.has("created_time"))
						continue;
					
					
					try {
						date.setTime(inputDateFormat.parse(post.getString("created_time")));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					
					if (date.getTimeInMillis() < minDate.getTimeInMillis() || date.getTimeInMillis() > maxDate.getTimeInMillis())
						continue;
					
					timePosts++;
					
					try {
						if (!post.has("place") || !post.getJsonObject("place").has("location"))
							continue;
					} catch (Exception e) {
						continue;
					}
					
					String country = null;
					String city = null;
					if (post.getJsonObject("place").getJsonObject("location").has("country")) {
						countryTimePosts++;
						country = post.getJsonObject("place").getJsonObject("location").getString("country");
					}
					if (post.getJsonObject("place").getJsonObject("location").has("city")) {
						cityTimePosts++;
						city = post.getJsonObject("place").getJsonObject("location").getString("city");
					}
					
					boolean spanishAdded = false;
					if (country != null) { 
						String language = languageMap.getLanguage(country);
						if (language != null && language.equals("es")) {
							spanishTimePosts++;
							spanishAdded = true;
						}
					}
					
					if (city != null && !spanishAdded) { 
						String language = languageMap.getLanguage(city);
						if (language != null && language.equals("es")) {
							spanishTimePosts++;
						}
					}
				}
		
			}
			System.out.println("Finished processing data file " + dataFileId);
		}
		
		System.out.println("Total Posts: " + totalPosts);
		System.out.println("Posts with time: " + timePosts + " (" + ((double)timePosts)/totalPosts + ")");
		System.out.println("Posts with time and country: " + countryTimePosts + " (" + ((double)countryTimePosts)/totalPosts + ")");
		System.out.println("Posts with time and city: " + cityTimePosts + " (" + ((double)cityTimePosts)/totalPosts + ")");
		System.out.println("Posts with time and spanish: " + spanishTimePosts + " (" + ((double)spanishTimePosts)/totalPosts + ")");
	}
	
	public static List<JsonObject> readData(File dataFile) {
		List<JsonObject> data = new ArrayList<JsonObject>();
		
		if (!dataFile.getName().contains("facebookPageData"))
			return null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(dataFile));
			String line = null;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				JsonObject dataObj = new JsonObject(line);
				data.add(dataObj);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		return data;
	}
}
