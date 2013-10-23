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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
	private static CharSequence FACEBOOK_IGNORE_URL = "https://graph.facebook.com/";
	private static SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static int MAX_PAGE_REQUESTS_PER_ITERATION = 50; // Each query has 4 facebook requests
	private static int MAX_FACEBOOK_REQUESTS_PER_BATCH = 50;
	private static int MAX_ERROR_RETRIES = 5;
	private static int ERROR_SLEEP_MILLIS = 1000*60*4; // 4 min
	
	public enum PageRequestType {
		MAIN,
		FEED,
		LIKES,
		EVENTS
	}
	
	public class PageRequest {
		private String pageId;
		private PageRequestType type;
		private int limit;
		private long until;
		private String __paging_token;
		private String after;
		
		public PageRequest() {
			this(null, null, -1, -1, null, null);
		}
		
		public PageRequest(String pageId, PageRequestType type) {
			this(pageId, type, -1, -1, null, null);
		}
		
		public PageRequest(String pageId, PageRequestType type, int limit, long until, String __paging_token, String after) {
			this.pageId = pageId;
			this.type = type;
			this.limit = limit;
			this.until = until;
			this.__paging_token = __paging_token;
			this.after = after;
		}
		
		public String getPageId() {
			return this.pageId;
		}
		
		public PageRequestType getType() {
			return this.type;
		}
		
		public int getLimit() {
			return this.limit;
		}
		
		public long getUntil() {
			return this.until;
		}
		
		public String get__paging_token() {
			return this.__paging_token;
		}
		
		public String getAfter() {
			return this.after;
		}
		
		public boolean fromString(String requestStr) {
			try {
				requestStr = requestStr.replace(FACEBOOK_IGNORE_URL, "");
				
				if (requestStr.indexOf("/") < 0) {
					this.pageId = requestStr;
					this.type = PageRequestType.MAIN;
					return true;
				}
				
				String[] requestStrParts = requestStr.split("/");
				String pageId = requestStrParts[0];
				String typeAndArgs = requestStrParts[1];
				if (!typeAndArgs.contains("?")) {
					this.pageId = pageId;
					this.type = PageRequestType.valueOf(typeAndArgs.toUpperCase());
					return true;
				}
				
				String[] typeAndArgsParts = typeAndArgs.split("\\?");
				PageRequestType type = PageRequestType.valueOf(typeAndArgsParts[0].toUpperCase());
				String args = typeAndArgsParts[1];
				String[] argAssignments = args.split("\\&|\\%3F");
				int limit = -1;
				long until = -1;
				String __paging_token = null;
				String after = null;
				for (int i = 0; i < argAssignments.length; i++) {
					if (!argAssignments[i].contains("="))
						continue;
					String[] assignmentParts = argAssignments[i].split("\\=|\\%3D");
					if (assignmentParts[0].equals("limit"))
						limit = Integer.parseInt(assignmentParts[1]);
					else if (assignmentParts[0].equals("until"))
						until = Integer.parseInt(assignmentParts[1]);
					else if (assignmentParts[0].equals("__paging_token"))
						__paging_token = assignmentParts[1];
					else if (assignmentParts[0].equals("after"))
						after = assignmentParts[1];
				}
				
				this.pageId = pageId;
				this.type = type;
				this.limit = limit;
				this.until = until;
				this.__paging_token = __paging_token;
				this.after = after;
				
				return true;
			} catch (Exception e) {
				writeLog("Failed to parse page request from string (" + requestStr + "): " + e.getMessage());
				return false;
			}
		}
		
		public String toString() {
			StringBuilder str = new StringBuilder();
			str = str.append(this.pageId);
			if (this.type == PageRequestType.MAIN)
				return str.toString();
			else if (this.type == PageRequestType.EVENTS)
				str = str.append("/events");
			else if (this.type == PageRequestType.LIKES)
				str = str.append("/likes");
			else if (this.type == PageRequestType.FEED)
				str = str.append("/feed");
			
			boolean firstArg = false;
			if (this.limit >= 0) {
				str = str.append("?limit=").append(this.limit);
				firstArg = true;
			}
			
			if (this.until >= 0) {
				if (firstArg)
					str = str.append("&until=").append(this.until);
				else
					str = str.append("?until=").append(this.until);
				firstArg = true;
			}
			
			if (this.__paging_token != null) {
				if (firstArg)
					str = str.append("&__paging_token=").append(this.__paging_token);
				else
					str = str.append("?__paging_token=").append(this.__paging_token);
				firstArg = true;
			}
			
			if (this.after != null) {
				if (firstArg)
					str = str.append("&after=").append(this.after);
				else
					str = str.append("?after=").append(this.after);
				firstArg = true;
			}
			
			return str.toString();
		}
	}
	
	public class PageResponse {
		private JsonObject response;
		private PageRequest sourceRequest;
		
		public PageResponse(PageRequest sourceRequest, JsonObject response) {
			this.sourceRequest = sourceRequest;
			this.response = response;
		}
		
		public String toString() {
			JsonObject fullObj = new JsonObject();
			fullObj.put("request", this.sourceRequest.toString());
			fullObj.put("id", this.sourceRequest.getPageId());
			fullObj.put("type", this.sourceRequest.getType());
			fullObj.put("limit", this.sourceRequest.getLimit());
			fullObj.put("until", this.sourceRequest.getUntil());
			fullObj.put("__paging_token", this.sourceRequest.get__paging_token());
			fullObj.put("after", this.sourceRequest.getAfter());
			fullObj.put("response", this.response);
			return fullObj.toString();
		}
		
		public JsonObject getResponse() {
			return this.response;
		}
		
		public PageRequest getSourceRequest() {
			return this.sourceRequest;
		}
	
		public PageRequest getNextPageRequest() {
			if (this.response.has("paging") && this.response.getJsonObject("paging").has("next")) {
				String nextRequestStr = this.response.getJsonObject("paging").getString("next");
				PageRequest nextRequest = new PageRequest();
				if (nextRequest.fromString(nextRequestStr))
					return nextRequest;
				else
					return null;
			} else {
				return null;
			}
		}
		
		public Set<String> getRelatedPageIds() {
			Set<String> likedPageIds = new HashSet<String>();
			if (this.sourceRequest.getType() != PageRequestType.LIKES)
				return likedPageIds;
			
			if (!this.response.has("data")) {
				writeLog("'Likes' response for " + this.sourceRequest.toString() + " missing data. Skipping...");
				return likedPageIds;
			}
			
			JsonArray likes = this.response.getJsonArray("data");
			for (int i = 0; i < likes.length(); i++) {
				JsonObject like = likes.getJsonObject(i);
				if (!like.has("id")) {
					writeLog("Page data like from request " + this.sourceRequest.toString() + " is missing id... Skipping...");
					continue;
				}
				likedPageIds.add(like.getString("id"));
			}
			
			return likedPageIds;
		}
	}
	
	private UnrestProperties properties;
	private File logFile;
	private File seedPageUrlsFile;
	private File currentRequestsFile;
	private File visitedPageIdsFile;
	private File pageDataDir;
	private File currentIterationFile;
	
	private int maxThreads;
	
	private FacebookClient client;

	public FacebookScraper() {
		this.properties = new UnrestProperties();
		this.logFile = new File(this.properties.getFacebookDataScrapeDirPath(), "Log");
		this.seedPageUrlsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "SeedPageUrls");
		this.currentRequestsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "CurrentRequests");
		this.visitedPageIdsFile = new File(this.properties.getFacebookDataScrapeDirPath(), "VisitedPageIds");
		this.pageDataDir = new File(this.properties.getFacebookDataScrapeDirPath(), "PageData");
		this.currentIterationFile = new File(this.properties.getFacebookDataScrapeDirPath(), "CurrentIteration");
		this.maxThreads = this.properties.getMaxThreads();
	}
	
	public void run() {
		run(1);
	}
	
	public void run(int iterations) {
		writeLog("Initializing...");
		
		initializeFacebookClient();
		LinkedList<PageRequest> currentRequests = loadPageRequests(this.currentRequestsFile);
		if (currentRequests == null)
			currentRequests = retrieveSeedRequests();
		Set<String> visitedPageIds = loadPageIds(this.visitedPageIdsFile);
		if (visitedPageIds == null)
			visitedPageIds = new HashSet<String>();

		for (PageRequest request : currentRequests)
			visitedPageIds.add(request.getPageId());
		
		int currentIteration = loadCurrentIteration();
		int maxIteration = currentIteration + iterations;
		while (currentIteration < maxIteration) {
			if (currentRequests.size() == 0) {
				writeLog("No outstanding requests... exiting.");
				break;
			}
			
			List<PageRequest> currentIterationRequests = new ArrayList<PageRequest>();
			while (currentIterationRequests.size() < FacebookScraper.MAX_PAGE_REQUESTS_PER_ITERATION && currentRequests.size() > 0)
				currentIterationRequests.add(currentRequests.removeFirst());
			
			List<PageResponse> pageResponses = sendPageRequests(currentIterationRequests);
			for (PageResponse pageResponse : pageResponses) {
				Set<String> relatedPageIds = pageResponse.getRelatedPageIds();
				for (String relatedPageId : relatedPageIds) {
					if (!visitedPageIds.contains(relatedPageId)) {
						writeLog("Adding requests for 'liked' page to queue: " + relatedPageId);
						
						currentRequests.addLast(new PageRequest(relatedPageId, PageRequestType.MAIN));
						currentRequests.addLast(new PageRequest(relatedPageId, PageRequestType.FEED));
						currentRequests.addLast(new PageRequest(relatedPageId, PageRequestType.LIKES));
						currentRequests.addLast(new PageRequest(relatedPageId, PageRequestType.EVENTS));
						visitedPageIds.add(relatedPageId);
					}
				}
				
				PageRequest nextPageRequest = pageResponse.getNextPageRequest();
				if (nextPageRequest != null) {
					writeLog("Adding next page request to queue: " + nextPageRequest);
					currentRequests.addFirst(nextPageRequest);
				}
			}
			
			
			if (!savePageResponseData(pageResponses, currentIteration)) {
				writeLog("Error: Failed to output page response data for iteration " + currentIteration);
				System.exit(0);
			}
			
			if (!savePageRequests(this.currentRequestsFile, currentRequests)) {
				writeLog("Error: Failed to output current requests for iteration");
				System.exit(0);
			}
			
			if (!savePageIds(this.visitedPageIdsFile, visitedPageIds)) {
				writeLog("Error: Failed to output visited page ids for iteration " + currentIteration);
				System.exit(0);
			}
			
			currentIteration++;
			if (!saveCurrentIteration(currentIteration)) {
				writeLog("Error: Failed to output current iteration to iteration file at iteration " + currentIteration);
				System.exit(0);
			}
		}
	}
	
	
	private List<PageResponse> sendPageRequests(List<PageRequest> requests) {
		writeLog("Setting up requests...");
		List<PageResponse> responses = new ArrayList<PageResponse>();
		String[] requestStrs = new String[requests.size()];
		for (int i = 0; i < requests.size(); i++) {
			requestStrs[i] = requests.get(i).toString();
		}
		
		String[] responseStrs = executeFacebookRequests(requestStrs);
		for (int i = 0; i < responseStrs.length; i++) {
			if (responseStrs[i] == null)
				continue;
			responses.add(new PageResponse(requests.get(i), new JsonObject(responseStrs[i])));
		}
		
		writeLog("Finished requests.");
		
		return responses;
	}
	
	private LinkedList<PageRequest> retrieveSeedRequests() {
		Set<String> seedPageUrlSet = loadSeedPageUrls();
		String[] seedPageUrls = new String[seedPageUrlSet.size()];
		int i = 0;
		for (String seedPageUrl : seedPageUrlSet) {
			seedPageUrls[i] = seedPageUrl;
			i++;
		}
		
		writeLog("Retrieving seed page requests for " + seedPageUrls.length + " pages...");
		
		String[] seedPageData = executeFacebookRequests(seedPageUrls);
		LinkedList<PageRequest> pageRequests = new LinkedList<PageRequest>();
		for (String seedPageDatum : seedPageData) {
			if (seedPageDatum == null)
				continue;
			JsonObject pageObj = new JsonObject(seedPageDatum);
			if (!pageObj.has("id"))
				continue;
			String pageId = pageObj.getString("id");
			if (!pageId.contains("/")) {
				pageRequests.add(new PageRequest(pageId, PageRequestType.MAIN));
				pageRequests.add(new PageRequest(pageId, PageRequestType.FEED));
				pageRequests.add(new PageRequest(pageId, PageRequestType.LIKES));
				pageRequests.add(new PageRequest(pageId, PageRequestType.EVENTS));
			}
		}
		writeLog("Retrieved " + pageRequests.size() + " seed page requests.");
		return pageRequests;
	}
	
	private class FacebookPartialRequestsThread implements Runnable {
		private String[] requests;
		private String[] responses;
		private int firstRequest;
		private int numRequests;
		
		public FacebookPartialRequestsThread(String[] requests, String[] responses, int firstRequest, int numRequests) {
			this.requests = requests;
			this.responses = responses;
			this.firstRequest = firstRequest;
			this.numRequests = numRequests;
		}
		
		@Override
		public void run() {
			try {
				String[] partialRequests = new String[this.numRequests];
				for (int i = this.firstRequest; i < this.firstRequest + this.numRequests; i++) {
					partialRequests[i - this.firstRequest] = this.requests[i];
				}
				
				String[] partialResponses = executeFacebookRequests(partialRequests);
				for (int i = this.firstRequest; i < this.firstRequest + this.numRequests; i++) {
					this.responses[i] = partialResponses[i - this.firstRequest];
				}
			} catch (OutOfMemoryError e) {
				writeLog("ERROR: Out of memory while processing requests...");
				System.exit(0);
			}
		}
	}
	
	private String[] executeFacebookRequests(String[] requests) {
		if (requests.length > FacebookScraper.MAX_FACEBOOK_REQUESTS_PER_BATCH) {
			ExecutorService threadPool = Executors.newFixedThreadPool(this.maxThreads);
			String[] responses = new String[requests.length];
			int totalRequested = 0;
			while (totalRequested < requests.length) {
				int toRequest = Math.min(totalRequested+FacebookScraper.MAX_FACEBOOK_REQUESTS_PER_BATCH, requests.length) - totalRequested;
				threadPool.submit(new FacebookPartialRequestsThread(requests, responses, totalRequested, toRequest));
				totalRequested += toRequest;
			}
			
			try {
				threadPool.shutdown();
				threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			
			return responses;
		}
		
		String[] responses = new String[requests.length];
		BatchRequest[] batchRequests = new BatchRequest[requests.length];
		for (int i = 0; i < batchRequests.length; i++) {
			writeLog("Executing Facebook request: " + requests[i]);
			batchRequests[i] = new BatchRequest.BatchRequestBuilder(requests[i]).build();
		}

		List<BatchResponse> batchResponses = null;
		int retries = 0;
		while (batchResponses == null && retries < FacebookScraper.MAX_ERROR_RETRIES) {
			try {
				batchResponses = this.client.executeBatch(batchRequests);
			} catch(FacebookOAuthException fe) {
				writeLog("Error: Failed to execute Facebook batch request (OAuth). Message: " + fe.getErrorMessage() + "... Getting new access token and retrying...");
				initializeFacebookClient();
				retries++;
			} catch(FacebookException fe) {
				try {
					writeLog("Error: Failed to execute Facebook batch request. Message: " + fe.getMessage() + "...  Sleeping for a bit and then retrying...");
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
			if (response == null) {
				writeLog("Error: Missing Facebook response for request " + batchRequests[i].getRelativeUrl() + ".  Skipping...");
				continue;
			} else if (response.getCode() == 200) {
				responses[i] = response.getBody();
			} else {
				// TODO Make constants for these later if used anywhere else
				// See: https://developers.facebook.com/docs/reference/api/errors/
				JsonObject responseBody = new JsonObject(response.getBody());
				if (!responseBody.has("error")) {
					writeLog("Error: Facebook request failed without error (" + responseBody.toString() + "). Skipping request " + batchRequests[i].getRelativeUrl());
					continue;
				}
				
				JsonObject errorObj = responseBody.getJsonObject("error");
				if (!errorObj.has("code")) {
					writeLog("Error: Facebook request failed with code-less error (" + errorObj.toString() + ") (" + responseBody.toString() +").  Skipping request " + batchRequests[i].getRelativeUrl());
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
					writeLog("Error: Facebook request failed for unknown reasons.  Skipping request " + batchRequests[i].getRelativeUrl());
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
	
	private LinkedList<PageRequest> loadPageRequests(File pageRequestsFile) {
		writeLog("Loading page requests...");
		try {
			if (!pageRequestsFile.exists())
				return null;
			
			BufferedReader br = new BufferedReader(new FileReader(pageRequestsFile));
			String line = null;
			LinkedList<PageRequest> pageRequests = new LinkedList<PageRequest>();
			
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0) {
					PageRequest request = new PageRequest();
					if (request.fromString(line))
						pageRequests.add(request);
				}
			}
			
			br.close();
			
			writeLog("Loaded " + pageRequests.size() + " page requests.");
			return pageRequests;
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
	
	private boolean savePageRequests(File pageRequestsFile, LinkedList<PageRequest> pageRequests) {
		writeLog("Saving page requests to file " + pageRequestsFile.getName() + "...");
		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(pageRequestsFile));
			
			for (PageRequest pageRequest : pageRequests) {
				w.write(pageRequest.toString() + "\n");
			}
			
			w.close();
	        return true;
	    } catch (IOException e) { e.printStackTrace(); return false; }
	}
	
	private boolean savePageResponseData(List<PageResponse> data, int iteration) {
		writeLog("Saving page response data for iteration " + iteration + "...");
		try {
			File pageDataFile = new File(this.pageDataDir.getAbsolutePath(), "facebookPageData_" + iteration);
			BufferedWriter w = new BufferedWriter(new FileWriter(pageDataFile));
			
			for (PageResponse datum : data) {
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
			w.write(iteration + "\n");
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
			int currentIteration = Integer.parseInt(br.readLine().trim());
			br.close();
			return currentIteration;
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	private synchronized void writeLog(String log) {
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
