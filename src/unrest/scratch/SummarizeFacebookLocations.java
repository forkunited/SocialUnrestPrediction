package unrest.scratch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import unrest.util.UnrestProperties;

import com.restfb.json.JsonArray;
import com.restfb.json.JsonObject;

public class SummarizeFacebookLocations {
	public static void main(String[] args) {
		UnrestProperties properties = new UnrestProperties(false);

		File inputDir = new File(properties.getFacebookDataScrapeDirPath(), "PageData");
		File[] dataFiles = inputDir.listFiles();
		int totalPosts = 0;
		int timePosts = 0;
		int countryTimePosts = 0;
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
					timePosts++;
					
					if (!post.has("place") || !post.getJsonObject("place").has("location"))
						continue;
					
					if (post.getJsonObject("place").getJsonObject("location").has("country"))
						countryTimePosts++;
					if (post.getJsonObject("place").getJsonObject("location").has("city"))
						cityTimePosts++;
				}
		
			}
			System.out.println("Finished processing data file " + dataFileId);
		}
		
		System.out.println("Total Posts: " + totalPosts);
		System.out.println("Posts with time: " + timePosts + " (" + ((double)timePosts)/totalPosts + ")");
		System.out.println("Posts with time and country: " + countryTimePosts + " (" + ((double)countryTimePosts)/totalPosts + ")");
		System.out.println("Posts with time and city: " + cityTimePosts + " (" + ((double)cityTimePosts)/totalPosts + ")");
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
