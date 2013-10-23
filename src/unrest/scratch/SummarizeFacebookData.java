package unrest.scratch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import unrest.detector.Detector;
import unrest.detector.DetectorBBN;
import unrest.util.UnrestProperties;

import com.restfb.json.JsonArray;
import com.restfb.json.JsonObject;

public class SummarizeFacebookData {
	public class FacebookPageSummary {
		private String id;
		private String name;
		private String category;
		private int postCount;
		private int eventCount;
		private int likeCount;
		private int unrestDetectedCount;
		private String examplePost;
		private String exampleLike;
		private String exampleEvent;
		private String exampleUnrest;
		
		public FacebookPageSummary(String id) {
			this.id = id;
			this.name = "";
			this.category = "";
			this.postCount = 0;
			this.eventCount = 0;
			this.likeCount = 0;
			this.unrestDetectedCount = 0;
			this.examplePost = "";
			this.exampleLike = "";
			this.exampleEvent = "";
			this.exampleUnrest = "";
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public void setCategory(String category) {
			this.category = category;
		}
		
		public void incrementPostCount() {
			this.postCount++;
		}
		
		public void incrementEventCount() {
			this.eventCount++;
		}
		
		public void incrementLikeCount() {
			this.likeCount++;
		}
		
		public void incrementUnrestDetectedCount() {
			this.unrestDetectedCount++;
		}
		
		public void setExamplePost(String post) {
			this.examplePost = post;
		}
		
		public void setExampleLike(String like) {
			this.exampleLike = like;
		}
		
		public void setExampleEvent(String event) {
			this.exampleEvent = event;
		}
		
		public void setExampleUnrest(String unrest) {
			this.exampleUnrest = unrest;
		}
		
		public String toString() {
			StringBuilder str = new StringBuilder();
			
			str.append(this.id);
			str.append("\t");
			str.append(this.name);
			str.append("\t");	
			str.append(this.category);
			str.append("\t");
			str.append(this.postCount);
			str.append("\t");
			str.append(this.eventCount);
			str.append("\t");
			str.append(this.likeCount);
			str.append("\t");
			str.append(this.unrestDetectedCount);
			str.append("\t");
			str.append(this.examplePost);
			str.append("\t");
			str.append(this.exampleEvent);
			str.append("\t");
			str.append(this.exampleLike);
			str.append("\t");
			str.append(this.exampleUnrest);
			str.append("\t");

			return str.toString();
		}
	}
	
	public SummarizeFacebookData() {
		
	}
	
	public static void main(String[] args) {
		Map<String, FacebookPageSummary> summaries = new HashMap<String, FacebookPageSummary>();
		DetectorBBN unrestDetector = new DetectorBBN();
		UnrestProperties properties = new UnrestProperties();
		File outputFile = new File(properties.getFacebookDataScrapeDirPath(), "FacebookDataSummary.tsv");
		SummarizeFacebookData summarize = new SummarizeFacebookData();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		Calendar date = Calendar.getInstance();
		
		File inputDir = new File(properties.getFacebookDataScrapeDirPath(), "PageData");
		File[] dataFiles = inputDir.listFiles();
		for (File dataFile : dataFiles) {
			List<JsonObject> data = readData(dataFile);
			if (data == null)
				continue;
			for (JsonObject datum : data) {
				String id = datum.getString("id");
				String type = datum.getString("type");
				JsonObject response = datum.getJsonObject("response");
				if (!summaries.containsKey(id))
					summaries.put(id, summarize.new FacebookPageSummary(id));
				FacebookPageSummary summary = summaries.get(id);
				
				if (type.equals("MAIN")) {
					String name = "", category = "";
					if (response.has("name"))
						name = response.getString("name");
					if (response.has("category"))
						category = response.getString("category");
					summary.setName(name);
					summary.setCategory(category);
				} else if (type.equals("FEED")) {
					JsonArray posts = response.getJsonArray("data");
					for (int i = 0; i < posts.length(); i++) {
						JsonObject post = posts.getJsonObject(i);
						if (!post.has("message"))
							continue;
						String message = post.getString("message");
						
						summary.incrementPostCount();
						summary.setExamplePost(post.getString("id"));
						
						if (!post.has("created_time"))
							continue;
						String createdTime = post.getString("created_time");
						try {
							date.setTime(dateFormat.parse(createdTime));
							Detector.Prediction prediction = unrestDetector.getPrediction(message, date);
							if (prediction != null) {
								summary.incrementUnrestDetectedCount();
								summary.setExampleUnrest(post.getString("id"));
							}
						} catch (Exception e) {
							
						}	
					}
				} else if (type.equals("LIKES")) {
					JsonArray likes = response.getJsonArray("data");
					for (int i = 0; i < likes.length(); i++) {
						JsonObject like = likes.getJsonObject(i);
						if (!like.has("id"))
							continue;
						summary.incrementLikeCount();
						summary.setExampleLike(like.getString("id"));
					}
				} else if (type.equals("EVENTS")) {
					JsonArray events = response.getJsonArray("data");
					for (int i = 0; i < events.length(); i++) {
						JsonObject event = events.getJsonObject(i);
						if (!event.has("id"))
							continue;
						summary.incrementEventCount();
						summary.setExampleEvent(event.getString("id"));
					}
				}
			}
		}
		
		outputSummaries(summaries, outputFile);
	}
	
	public static void outputSummaries(Map<String, FacebookPageSummary> summaries, File outputFile) {
		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
			w.write("Id\tName\tCategory\tPosts\tEvents\tLikes\tUnrest\tEx. Post\tEx. Event\tEx. Like\tEx. Unrest\n");
			
			for (Entry<String, FacebookPageSummary> entry : summaries.entrySet()) {
				String summaryStr = entry.toString();
				w.write(summaryStr + "\n");
				System.out.println(summaryStr);
			}
			w.close();
	    } catch (IOException e) { 
	    	e.printStackTrace(); 
	    }
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
