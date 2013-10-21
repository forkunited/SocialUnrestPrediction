package unrest.facebook;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.FacebookClient.AccessToken;
import com.restfb.batch.BatchRequest;
import com.restfb.batch.BatchResponse;
import com.restfb.exception.FacebookException;
import com.restfb.exception.FacebookOAuthException;
import com.restfb.json.JsonArray;
import com.restfb.json.JsonObject;

import unrest.util.UnrestProperties;

/**
 * FacebookScraper crawls over Facebook pages that like each other
 * 
 * Notes:
 * Page data contains: data returned from [id], [id]/likes, [id]/feed, [id]/events
 * Wanted to do the following:
 * 	- For each user who likes or comments on each post, get the user.	
 *	- For each user, get all liked and commented organizations
 *	- But each user likes usually returns [BLANK] due to privacy...
 * 
 * @author Bill
 *
 */

public class FacebookScraper {
	private static SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static int MAX_PAGE_QUERIES_PER_ITERATION = 200;
	private static int MAX_ERROR_RETRIES = 5;
	private static int ERROR_SLEEP_MILLIS = 1000*60*10; // 10 min
	
	private UnrestProperties properties;
	private File logFile;
	private File seedPageUrlsFile;
	private File currentPageIdsFile;
	private File visitedPageIdsFile;
	private File pageDataDir;
	private File currentIterationFile;
	
	private FacebookClient client;
	
	
	public FacebookScraper() {
		this.properties = new UnrestProperties();
		this.logFile = new File(this.properties.getFacebookDataScrapeDirPath(), "Log");
		this.seedPageUrlsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "SeedPageUrls");
		this.currentPageIdsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "CurrentPageIds");
		this.visitedPageIdsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "VisitedPageIds");
		this.pageDataDir = new File(this.properties.getFacebookDataScrapeDirPath(), "PageData");
		this.currentIterationFile = new File(this.properties.getFacebookDataScrapeDirPath(), "CurrentIteration");
	}
	
	public void run() {
		run(-1);
	}
	
	public void run(int iterations) {
		writeLog("Initializing...");
		
		initializeFacebookClient();
		Set<String> currentPageIds = loadPageIds(this.currentPageIdsFile);
		if (currentPageIds == null)
			currentPageIds = retrieveSeedPageIds();
		Set<String> visitedPageIds = loadPageIds(this.visitedPageIdsFile);
		if (visitedPageIds == null)
			visitedPageIds = new HashSet<String>();
		
		Queue<String> pageIdsToQuery = new LinkedList<String>();
		pageIdsToQuery.addAll(currentPageIds);
		int currentIteration = loadCurrentIteration();
		for (int i = 0; i < iterations; i++) {
			if (pageIdsToQuery.size() == 0) {
				writeLog("No page ids to query... exiting.");
				break;
			}
			
			Set<String> currentIterationPageIds = new HashSet<String>();
			while (currentIterationPageIds.size() < FacebookScraper.MAX_PAGE_QUERIES_PER_ITERATION && pageIdsToQuery.size() > 0)
				currentIterationPageIds.add(pageIdsToQuery.remove());
			
			List<JsonObject> pageData = retrievePageData(currentIterationPageIds);
			Set<String> likedPageIds = getLikedPageIds(pageData);
			
			for (String likedPageId : likedPageIds)
				if (!visitedPageIds.contains(likedPageId) && !pageIdsToQuery.contains(likedPageId))
					pageIdsToQuery.add(likedPageId);
			
			for (String pageId : currentIterationPageIds)
				visitedPageIds.add(pageId);
			
			if (!savePageData(pageData, i + currentIteration)) {
				writeLog("Error: Failed to output page data for iteration " + currentIteration);
				System.exit(0);
			}
			
			if (!savePageIds(this.visitedPageIdsFile, visitedPageIds)) {
				writeLog("Error: Failed to output visited page ids for iteration " + currentIteration);
				System.exit(0);
			}
			
			if (!savePageIds(this.currentPageIdsFile, new HashSet<String>(pageIdsToQuery))) {
				writeLog("Error: Failed to output current page ids for iteration");
				System.exit(0);
			}
			
			currentIteration++;
			if (!saveCurrentIteration(currentIteration)) {
				writeLog("Error: Failed to output current iteration to iteration file at iteration " + currentIteration);
				System.exit(0);
			}
		}
	}
	
	private Set<String> getLikedPageIds(List<JsonObject> pageData) {
		writeLog("Getting liked page ids from page data...");
		Set<String> pageIds = new HashSet<String>();
		for (JsonObject pageDatum : pageData) {
			if (!pageDatum.has("likes") || !pageDatum.getJsonObject("likes").has("data")) {
				writeLog("Page data is missing likes (" + pageDatum.getJsonObject("main").getString("id") + ").  Skipping...");
				continue;
			}
			JsonArray likes = pageDatum.getJsonObject("likes").getJsonArray("data");
			for (int i = 0; i < likes.length(); i++) {
				JsonObject like = likes.getJsonObject(i);
				if (!like.has("id")) {
					writeLog("Page data like is missing id (" + pageDatum.getJsonObject("main").getString("id") + "). Skipping...");
					continue;
				}
				pageIds.add(like.getString("id"));
			}
		}
		return pageIds;
	}
	
	private List<JsonObject> retrievePageData(Set<String> pageIds) {
		writeLog("Retrieveing page data...");
		String[] pageDataRequests = new String[pageIds.size()*4];
		int i = 0;
		for (String pageId : pageIds) {
			pageDataRequests[i++] = pageId;
			pageDataRequests[i++] = pageId + "/likes";
			pageDataRequests[i++] = pageId + "/feed";
			pageDataRequests[i++] = pageId + "/events";
		}
		
		Map<String, JsonObject> pageData = new HashMap<String, JsonObject>();
		String[] pageDataResponses = executeFacebookRequests(pageDataRequests);
		for (i = 0; i < pageDataResponses.length; i++) {
			if (pageDataResponses[i] == null)
				continue;
			if (!pageDataRequests[i].contains("/")) {
				String[] requestParts = pageDataRequests[i].split("/");
				String pageId = requestParts[0];
				String requestType = requestParts[1];
				if (!pageData.containsKey(pageId))
					pageData.put(pageId, new JsonObject());
				pageData.get(pageId).put(requestType, new JsonObject(pageDataResponses[i]));
			} else {
				String pageId = pageDataRequests[i];
				if (!pageData.containsKey(pageId))
					pageData.put(pageId, new JsonObject());
				pageData.get(pageId).put("main", new JsonObject(pageDataResponses[i]));
			}
		}
		
		return new ArrayList<JsonObject>(pageData.values());
	}
	
	private Set<String> retrieveSeedPageIds() {
		Set<String> seedPageUrlSet = loadSeedPageUrls();
		String[] seedPageUrls = new String[seedPageUrlSet.size()];
		int i = 0;
		for (String seedPageUrl : seedPageUrlSet) {
			seedPageUrls[i] = seedPageUrl;
			i++;
		}
		
		writeLog("Retrieving seed page ids for " + seedPageUrls.length + " pages...");
		
		String[] seedPageData = executeFacebookRequests(seedPageUrls);
		Set<String> pageIds = new HashSet<String>();
		for (String seedPageDatum : seedPageData) {
			if (seedPageDatum == null)
				continue;
			JsonObject pageObj = new JsonObject(seedPageDatum);
			if (!pageObj.has("id"))
				continue;
			pageIds.add(pageObj.getString("id"));
		}
		writeLog("Retrieved " + pageIds.size() + " pages.");
		return pageIds;
	}
	
	private String[] executeFacebookRequests(String[] requests) {
		writeLog("Executing Facebook requests...");
		String[] responses = new String[requests.length];
		BatchRequest[] batchRequests = new BatchRequest[requests.length];
		for (int i = 0; i < batchRequests.length; i++) {
			batchRequests[i] = new BatchRequest.BatchRequestBuilder(requests[i]).build();
		}

		List<BatchResponse> batchResponses = null;
		int retries = 0;
		while (batchResponses == null && retries < FacebookScraper.MAX_ERROR_RETRIES) {
			try {
				batchResponses = this.client.executeBatch(batchRequests);
			} catch(FacebookOAuthException fe) {
				writeLog("Error: Failed to execute Facebook batch request (OAuth).  Getting new access token and retrying...");
				initializeFacebookClient();
				retries++;
			} catch(FacebookException fe) {
				try {
					writeLog("Error: Failed to execute Facebook batch request.  Sleeping for a bit and then retrying...");
					Thread.sleep(FacebookScraper.ERROR_SLEEP_MILLIS);
					retries++;
				} catch (Exception e) {
					writeLog("Error: Had too much trouble sleeping.  Exiting...");
					System.exit(0);
				}
			}
		}
		
		if (batchResponses == null) {
			writeLog("Error: Too many failed batch request attempts.  Exiting...");
			System.exit(0);
		}
		
		for (int i = 0; i < batchResponses.size(); i++) {
			BatchResponse response = batchResponses.get(i);
			if (response.getCode() == 200) {
				responses[i] = response.getBody();
			} else {
				// TODO Make constants for these later if used anywhere else
				// See: https://developers.facebook.com/docs/reference/api/errors/
				JsonObject errorObj = new JsonObject(response.getBody());
				if (!errorObj.has("code")) {
					writeLog("Error: Facebook request failed with code-less error.  Skipping request " + batchRequests[i]);
					continue; // Skip	
				}
				int errorCode = errorObj.getInt("code");
				if (errorCode == 190 	// OAuth
				 || errorCode == 102	// API Session
				) { 
					writeLog("Error: Facebook request failed (OAuth).  Getting new access token and retrying...");
					initializeFacebookClient();
				} else if (errorCode == 1	// API Unknown
						|| errorCode == 2	// API Service
						|| errorCode == 4	// API Too Many Calls
						|| errorCode == 17  // API User Too Many Calls
				) {
					try {
						writeLog("Error: Facebook request failed. Waiting a bit and then retrying...");
						Thread.sleep(FacebookScraper.ERROR_SLEEP_MILLIS);
					} catch (Exception e) {
						writeLog("Error: Had too much trouble sleeping. Exiting...");
						System.exit(0);
					}
				} else {
					writeLog("Error: Facebook request failed for unknown reasons.  Skipping request " + batchRequests[i]);
					continue; // Skip it
				}
				
				String[] remainingRequests = Arrays.copyOfRange(requests, i, requests.length);
				String[] remainingResponses = executeFacebookRequests(remainingRequests);
				for (int j = i; j < responses.length; j++)
					responses[j] = remainingResponses[j-i];
						
				return responses;
			}
		}
		
		return responses;
	}
	
	private void initializeFacebookClient() {
		writeLog("Setting up Facebook client...");
		AccessToken accessToken = new DefaultFacebookClient().obtainAppAccessToken(this.properties.getFacebookAppID(), this.properties.getFacebookAppSecret());
		this.client = new DefaultFacebookClient(accessToken.getAccessToken());
	}
	
	private Set<String> loadSeedPageUrls() {
		writeLog("Loading seed page urls...");
		try {
			BufferedReader br = new BufferedReader(new FileReader(this.seedPageUrlsFile));
			String line = null;
			Set<String> pageUrls = new HashSet<String>();
			
			while ((line = br.readLine()) != null) {
				String[] lineParts = line.trim().split("\t");
				if (lineParts.length < 2) {
					br.close();
					return null;
				}
				pageUrls.add(lineParts[1]);
			}
			
			br.close();
			
			writeLog("Loaded " + pageUrls.size() + " page urls.");
			return pageUrls;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private Set<String> loadPageIds(File pageIdsFile) {
		writeLog("Loading page ids...");
		try {
			if (!pageIdsFile.exists())
				return null;
			
			BufferedReader br = new BufferedReader(new FileReader(pageIdsFile));
			String line = null;
			Set<String> pageIds = new HashSet<String>();
			
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0)
					pageIds.add(line);
			}
			
			br.close();
			
			writeLog("Loaded " + pageIds.size() + " page ids.");
			return pageIds;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private boolean savePageIds(File pageIdsFile, Set<String> pageIds) {
		writeLog("Saving page ids to file " + pageIdsFile.getName() + "...");
		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(pageIdsFile));
			
			for (String pageId : pageIds) {
				w.write(pageId + "\n");
			}
			
			w.close();
	        return true;
	    } catch (IOException e) { e.printStackTrace(); return false; }
	}
	
	private boolean savePageData(List<JsonObject> data, int iteration) {
		writeLog("Saving page data for iteration " + iteration + "...");
		try {
			File pageDataFile = new File(this.pageDataDir.getAbsolutePath(), "facebookPageData_" + iteration);
			BufferedWriter w = new BufferedWriter(new FileWriter(pageDataFile));
			
			for (JsonObject datum : data) {
				w.write(datum.toString() + "\n");
			}
			
			w.close();
	        return true;
	    } catch (IOException e) { e.printStackTrace(); return false; }
	}

	private boolean saveCurrentIteration(int iteration) {
		writeLog("Saving current iteration (" + iteration + ")...");
		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(this.currentIterationFile));
			w.write(iteration);
			w.close();
	        return true;
	    } catch (IOException e) { e.printStackTrace(); return false; }
	}
	
	private int loadCurrentIteration() {
		writeLog("Loading current iteration...");
		try {
			if (!this.currentIterationFile.exists())
				return 0;
			
			BufferedReader br = new BufferedReader(new FileReader(this.currentIterationFile));
			int currentIteration = Integer.parseInt(br.readLine());
			br.close();
			return currentIteration;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	private void writeLog(String log) {
		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(this.logFile, true));
			String logStr = LOG_DATE_FORMAT.format(Calendar.getInstance().getTime()) + "\t" + log;
			w.write(logStr + "\n");
			System.out.println(logStr);
			w.close();
	    } catch (IOException e) { 
	    	e.printStackTrace(); 
	    }
	}
}
