package unrest.scratch;

import unrest.feature.UnrestFeature;
import unrest.feature.UnrestFeatureConjunction;
import unrest.feature.UnrestFeatureFutureDate;
//import unrest.feature.UnrestFeatureGazetteer;
//import unrest.feature.UnrestFeatureTotal;
import unrest.feature.UnrestFeatureUnigram;
//import unrest.util.FutureDateTextFinder;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
//import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Scratch {
	public static void main(String[] args) {
		/*try {
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
		}*/
		/*
		String s = "Declaran ante la justicia los hermanos David y Jos? Cavaleiro, los dos polic?as acusados por el episodio ocurrido el domingo en saavedra en el que Eric Milton Ponce result? gravemente herido por un disparo. el joven permanece internado en el hospital pirovano en grave estado.";
		DetectorBBN detector = new DetectorBBN();
		StringUtil.StringTransform cleanFn = StringUtil.getDefaultCleanFn();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		Calendar date = Calendar.getInstance();
		try {date.setTime(dateFormat.parse("2013-10-01T17:29:04+0000")); } catch (Exception e) {}
		
		Detector.Prediction p = detector.getPrediction(cleanFn.transform(s), date);
		System.out.println(p);*/
		//System.out.println("mañana");
		
		/*
		 * FutureDateTextFinder d = new FutureDateTextFinder(Calendar.getInstance());
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		List<Calendar> dates = d.findFutureDates("30 de novembro");
		//2013-04-09T16:44:22+0000
		System.out.println(df.format(dates.get(0).getTime()));
		dates.get(0).add(Calendar.DAY_OF_MONTH, 1);
		
		System.out.println(df.format(dates.get(0).getTime()));
		System.out.println(dates.get(0).get(Calendar.DATE));
		 */
		
		UnrestFeature tom = new UnrestFeatureFutureDate(true);
		UnrestFeature unigram = new UnrestFeatureUnigram();
		UnrestFeature unigramTom = new UnrestFeatureConjunction("unigramTom", unigram, tom);
		
		Calendar c = Calendar.getInstance();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		try {
			c.setTime(df.parse("2013-04-09T16:44:22+0000"));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Map<String, Integer> values = unigramTom.compute("10 de abril protesta", c, null);
		for (Entry<String, Integer> entry : values.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}
}
