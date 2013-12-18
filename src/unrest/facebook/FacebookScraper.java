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
 * FacebookScraper crawls across the Facebook page "like" network, scraping and 
 * storing page metadata, likes, feeds, and events using the RestFB (http://restfb.com/)
 * library to connect to the Facebook Graph API.
 * 
 * Initially, we had intended this class to crawl from unrest group pages, to users
 * who "like" those pages, to other pages that those users "like".  While it is possible
 * to crawl from the pages to the users who "like" and comment on the pages, it is unfortunately
 * not possible to crawl from users to pages that they "like"--user content is generally
 * not accessible through the Graph API without the proper access token.
 * (See the Graph API documentation at https://developers.facebook.com/docs/graph-api/ for more
 * information.)
 * 
 * Roughly, the "like" network traversed by FacebookScraper is a network of Facebook pages connected
 * by "like" relationships (a "like" relationship occurs when one Facebook page "Facebook-likes" another).
 * The more precise picture is more complicated, though, because each page consists of likes, events,
 * and feed data which can be "paged" if there is a lot of it ("paged" in the sense that there are several
 * separate "pages" of feed data on a given Facebook page, for example).  The FacebookScraper actually
 * crawls across a network that consists of "like" relationships plus links between "paged" data on 
 * a Facebook page.  To be more mathy, this network can be expressed as G=(V,E) where V=(F U M U N U L) and
 * 
 * F = {f_{pi} | f_{pi} is the ith "page" of feed on Facebook page p}
 * M = {m_{pi} | m_{pi} is the ith "page" of meta-data on Facebook page p}
 * N = {n_{pi} | n_{pi} is the ith "page" of events on Facebook page p}
 * L = {l_{pi} | l_{pi} is the ith "page" of likes on Facebook page p}
 * 
 * For a Facebook page p, there are edges in E:
 * 
 * (f_{pi}, f_{p(i+1)}) between consecutive feed "pages" on Facebook page p
 * (m_{pi}, m_{p(i+1)}) between consecutive meta-data "pages" on Facebook page p
 * (n_{pi}, n_{p(i+1)}) between consecutive event "pages" on Facebook page p
 * (l_{pi}, l_{p(i+1)}) between consecutive like "pages" on Facebook page p
 * 
 * Let E_p be the set of all of these edges between consecutive "pages" for all Facebook pages.
 * 
 * And there are also "like" edges.  The "like" edges representing the "like" relationship between pages p and p' are:
 * 
 * (l_{pi}, f_{p'0}) between the ith "like" "page" of p and the first feed "page" of p'.
 * (l_{pi}, m_{p'0}) between the ith "like" "page" of p and first meta-data "page" of p'.
 * (l_{pi}, n_{p'0}) between the ith "like" "page" of p and the first event "page" of p'.
 * (l_{pi}, l_{p'0}) between the ith "like" "page" of p and the first "like" "page" of p'.
 * 
 * Let E_l be the set of all of these edges between like "pages" and other kinds of pages for all Facebook pages.
 * 
 * The FacebookScraper is initialized with an initial set of Facebook pages P_{seed}, and it maintains a list Q of
 * nodes in the "like" network G defined above.  Q acts as a list of "pages" to request from the Facebook API in 
 * batches.  Basically, the scraper repeatedly takes a bunch of nodes from the front of Q, sends Facebook requests 
 * for them, stores the results of the requests, and updates Q to contain new nodes from G that were linked to 
 * the nodes for which requests were just sent.  
 * 
 * More precisely, the scraper starts by adding V_{seed}=(F_{seed} U M_{seed} U N_{seed} U L_{seed}) to Q where
 * F_{seed}={f_pi | p in P_{seed}}, and M_{seed}, N_{seed}, and L_{seed} are defined analogously. Then, while
 * Q is non-empty, the scraper repeats iterations of:
 * 
 * 1. Remove the first n elements Q_n of Q.
 * 2. For each element in Q_n, send a request, and stored the retrieved data.
 * 3. For each edge (q, r) in E_p that is adjacent to an element q in Q_n, add r to the front of Q.
 * 4. For each edge (q, r) in E_l that is adjacent to an element q in Q_n, add r to the back of Q.
 * 
 * Notice that step 3 adds consecutive "pages" to front of Q, and step 4 adds linked Facebook pages to the back
 * of Q.  This allows the scraper to download all of the "pages" for large Facebook pages (pages with years and
 * years of data) without getting stuck on them before traversing to other parts of the "like" network.
 * 
 * The scraper stores a separate file of scraped data for each iteration.  Each file contains a JSON object for
 * every node in G for which it sent a request.  This means that the data for a single Facebook page can
 * be spread across the several iterations' files.  
 * 
 * @author Bill McDowell
 *
 */

public class FacebookScraper {
	private static CharSequence FACEBOOK_IGNORE_URL = "https://graph.facebook.com/";
	private static SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static int MAX_PAGE_REQUESTS_PER_ITERATION = 50; //50;
	private static int MAX_FACEBOOK_REQUESTS_PER_BATCH = 10; //50; // Changed this to 10 to get rid of weird HTTP 504 errors
	private static int MAX_ERROR_RETRIES = 5;
	private static int ERROR_SLEEP_MILLIS = 1000*60*4; // 4 min
	
	
	// Types of requests to send to Facebook
	public enum PageRequestType {
		MAIN, // Meta-data
		FEED,
		LIKES,
		EVENTS
	}
	
	/*
	 * Represents a request for a single "page" of data from a Facebook page
	 */
	public class PageRequest {
		private String pageId;
		private PageRequestType type;
		
		// All of these attributes are for paging data. Facebook uses various representations for "pages".
		// Sometimes it uses "limit" and "until", and other times it uses "__paging_token" or "after"
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
	
	/*
	 *  Represents a response to a page request
	 */
	
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
	private File logFile; // Stores logs of what the scraper has done and errors that it has encountered
	private File seedPageUrlsFile; // URLs for seed Facebook pages
	private File currentRequestsFile; // Hold requests that should be made to Facebook in the future (Q in the description above)
	private File visitedPageIdsFile; // Holds Facebook ids of pages that have already been visited 
	private File pageDataDir; // Stores files containing data retrieved from Facebook
	private File currentIterationFile; // Stores the number of the current iteration
	
	private int maxThreads;
	
	private FacebookClient client;

	public FacebookScraper() {
		this.properties = new UnrestProperties(false);
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
	
	/*
	 *  Main method... runs the scraper for the specified number of iterations
	 */
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
	
	/*
	 * Sends a batch of "page" requests
	 */
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
	
	/*
	 * The initial seed set of Facebook pages is given by their URLs.  This metho retrieves the Facebook ids for
	 * this initial set, and builds requests using these ids
	 */
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
	
	/*
	 * Thread for sending a batch of requests to Facebook.  This is used by the "executeFacebookRequests"
	 * method below to execute several batches of requests at the same time if there are too many requests
	 * to send in a single batch.  However, this is somewhat useless now because Facebook seems like it
	 * started giving HTTP 504 errors when receiving multiple batches at once.  In response to this,
	 * executeFacebookRequests is now synchronized, so it only sends one batch at a time even if there
	 * are multiple threads.  It might be good to experiment with this later to see if we can still possibly
	 * send multiple batches at the same time and get rid of the 504 errors (the 504 errors didn't happen
	 * at first for several months, but then they suddenly started occurring...)
	 */
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
	
	/*
	 * Executes a batch of Facebook requests
	 */
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
		List<BatchResponse> batchResponses = null;
		synchronized (this) { // Not threaded for now because getting mysterious 504 errors when sending more than once batch at a time
			for (int i = 0; i < batchRequests.length; i++) {
				writeLog("Executing Facebook request: " + requests[i]);
				batchRequests[i] = new BatchRequest.BatchRequestBuilder(requests[i]).build();
			}
			
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
						writeLog("Error: Failed to execute Facebook batch request. Message: " + fe.getMessage() + "...  Sleeping for a bit (" + FacebookScraper.ERROR_SLEEP_MILLIS + ") and then retrying...");
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
			} else {
				writeLog("Successfully executed a batch of requests.");
			}
			
			for (int i = 0; i < batchResponses.size(); i++) {
				BatchResponse response = batchResponses.get(i);
				if (response == null) {
					writeLog("Error: Missing Facebook response for request " + batchRequests[i].getRelativeUrl() + ".  Retrying...");
					
					/// Try retrying instead of skipping...
					String[] remainingRequests = Arrays.copyOfRange(requests, i, requests.length);
					String[] remainingResponses = executeFacebookRequests(remainingRequests);
					for (int j = i; j < responses.length; j++)
						responses[j] = remainingResponses[j-i];
					return responses;
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
					} else if (/*errorCode == 1	// API Unknown
							|| errorCode == 2	// API Service
							|| */errorCode == 4	// API Too Many Calls
							|| errorCode == 17  // API User Too Many Calls
							|| errorCode == 613 // FQL Rate limit exceeded
					) {
						try {
							writeLog("Error: Facebook request failed. Waiting a bit and then retrying...");
							Thread.sleep(FacebookScraper.ERROR_SLEEP_MILLIS);
						} catch (Exception e) {
							writeLog("Error: Had too much trouble sleeping. Exiting...");
							System.exit(0);
						}
					} else {
						writeLog("Error: Facebook request failed for unknown reasons (Code: " + errorCode + ").  Skipping request " + batchRequests[i].getRelativeUrl());
						continue; // Skip it
					}
					
					String[] remainingRequests = Arrays.copyOfRange(requests, i, requests.length);
					String[] remainingResponses = executeFacebookRequests(remainingRequests);
					for (int j = i; j < responses.length; j++)
						responses[j] = remainingResponses[j-i];
							
					return responses;
				}
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
