package unrest.scratch;

import unrest.facebook.FacebookScraper;

public class RunFacebookScraper {
	public static void main(String[] args) {
		FacebookScraper scraper = new FacebookScraper();
		scraper.run(Integer.parseInt(args[0]));
	}
}