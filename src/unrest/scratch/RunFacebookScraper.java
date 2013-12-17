package unrest.scratch;

import unrest.facebook.FacebookScraper;

/**
 * Runs unrest.facebook.FacebookScraper for a specified number of iterations
 * @author Bill McDowell
 */
public class RunFacebookScraper {
	public static void main(String[] args) {
		FacebookScraper scraper = new FacebookScraper();
		scraper.run(Integer.parseInt(args[0]));
	}
}