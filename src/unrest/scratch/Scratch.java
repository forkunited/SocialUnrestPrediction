package unrest.scratch;

import unrest.twitter.JsonTweet;
import java.io.BufferedReader;
import java.io.FileReader;

public class Scratch {
	public static void main(String[] args) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("C:/Users/Bill/Documents/projects/NoahsARK/osi/Data/Twitter/testTweet.txt"));
			String line = reader.readLine();
			JsonTweet tweet = JsonTweet.readJsonTweet(line);
				
			if (tweet != null) {
				System.out.println(tweet.getText());
			} else {
				System.out.println("Shit.");
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
