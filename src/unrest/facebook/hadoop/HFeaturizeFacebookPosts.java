package unrest.facebook.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import ark.data.Gazetteer;

import unrest.feature.UnrestFeature;
import unrest.feature.UnrestFeatureConjunction;
import unrest.feature.UnrestFeatureFutureDate;
import unrest.feature.UnrestFeatureGazetteer;
import unrest.feature.UnrestFeatureTotal;
import unrest.feature.UnrestFeatureUnigram;
import unrest.util.UnrestProperties;

/**
 * Takes in lines output from HFilterFacebookDataToPosts of the form:
 * 
 * [Facebook Page ID]	[Facebook Post Object]
 * 
 * And outputs lines of the form:
 * 
 * [Date]	[Feature Type]	[Location]	[Feature Term]	[Count]
 * 
 * This output can be used as one of the inputs to David's social unrest prediction 
 * tool (prediction, but not training).
 */
public class HFeaturizeFacebookPosts {
	private static String languageFilter = null;
	
	public static class FeaturizeFacebookPostsMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text key = new Text();
		private IntWritable value = new IntWritable();
		
		private List<UnrestFeature> features;
		private UnrestProperties properties; 
		private Gazetteer cityGazetteer; 
		private Gazetteer countryGazetteer; 
		private Gazetteer cityCountryMapGazetteer; 
		private Gazetteer locationLanguageMapGazetteer;
		private SimpleDateFormat inputDateFormat; 
		private SimpleDateFormat outputDateFormat; 
		private Calendar date; 
		
		protected List<UnrestFeature> constructFeatures(String propertiesPath) {
			UnrestProperties properties = new UnrestProperties(true, propertiesPath);
			Gazetteer unrestTerms = new Gazetteer("UnrestTermLarge", properties.getUnrestTermLargeGazetteerPath());
			
			UnrestFeature tom = new UnrestFeatureFutureDate(true);
			
			UnrestFeature total = new UnrestFeatureTotal();
			UnrestFeature hand = new UnrestFeatureGazetteer(unrestTerms);
			UnrestFeature unigram = new UnrestFeatureUnigram();
			UnrestFeature handTom = new UnrestFeatureConjunction("handTom", hand, tom);
			UnrestFeature unigramTom = new UnrestFeatureConjunction("unigramTom", unigram, tom);
			
			List<UnrestFeature> features = new ArrayList<UnrestFeature>();
			features.add(total);
			features.add(hand);
			features.add(unigram);
			features.add(handTom);
			features.add(unigramTom);
			
			return features;
		}
		
		public void setup(Context context) {
			String propertiesPath = context.getConfiguration().get("PROPERTIES_PATH");
			this.features = constructFeatures(propertiesPath);
			this.properties = new UnrestProperties(true, propertiesPath);
			this.cityGazetteer = new Gazetteer("City", this.properties.getCityGazetteerPath());
			this.countryGazetteer = new Gazetteer("Country", this.properties.getCountryGazetteerPath());
			this.cityCountryMapGazetteer = new Gazetteer("CityCountryMap", this.properties.getCityCountryMapGazetteerPath());
			this.locationLanguageMapGazetteer = new Gazetteer("LocationLanguageMap", this.properties.getLocationLanguageMapGazetteerPath());
			this.inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
			this.outputDateFormat = new SimpleDateFormat("MM-dd-yyyy");
			this.date = Calendar.getInstance();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineParts = line.split("\\t");
			if (lineParts.length < 2)
				return;
			JSONObject lineObj = JSONObject.fromObject(lineParts[1]);
			
			if (!lineObj.getString("type").equals("POST"))
				return;
			
			String city = getCity(lineObj);
			String country = getCountry(lineObj, city);
			
			if ((city == null && country == null) || !validLanguage(city, country))
				return;
			
			try {
				this.date.setTime(this.inputDateFormat.parse(lineObj.getString("date")));
			} catch (ParseException e) {
				return;
			}
			
			String message = lineObj.getString("message");
			
			for (UnrestFeature feature : this.features) {
				String featureName = feature.getName();
				Map<String, Integer> featureValues = feature.compute(message, this.date, null); // FIXME: If necessary later, compute separately for city and country
				for (Entry<String, Integer> featureValue : featureValues.entrySet()) {
					if (featureValue.getKey().trim().length() == 0)
						continue;
					
					/* Output for city */
					if (city != null) {
						StringBuilder keyStr = new StringBuilder();
						keyStr = keyStr.append(this.outputDateFormat.format(this.date.getTime())).append("\t");
						keyStr = keyStr.append(featureName).append("\t");
						keyStr = keyStr.append(city).append("\t");
						keyStr = keyStr.append(featureValue.getKey());
						
						this.key.set(keyStr.toString().trim());
						this.value.set(featureValue.getValue());
						context.write(this.key, this.value);
					}
					
					/* Output for country */
					if (country != null) {
						StringBuilder keyStr = new StringBuilder();
						keyStr = keyStr.append(this.outputDateFormat.format(this.date.getTime())).append("\t");
						keyStr = keyStr.append(featureName).append("\t");
						keyStr = keyStr.append(country).append("\t");
						keyStr = keyStr.append(featureValue.getKey());
						
						this.key.set(keyStr.toString().trim());
						this.value.set(featureValue.getValue());
						context.write(this.key, this.value);
					}
				}
			}
		}
		
		private String getCity(JSONObject postObj) {
			if (!postObj.has("metadata") ||
				!postObj.getJSONObject("metadata").has("location") ||
				!postObj.getJSONObject("metadata").getJSONObject("location").has("city"))
				return null;
			
			String uncleanCity = postObj.getJSONObject("metadata").getJSONObject("location").getString("city");
			if (uncleanCity == null)
				return null;
			
			List<String> cleanCities = this.cityGazetteer.getIds(uncleanCity);
			if (cleanCities != null && !cleanCities.isEmpty())
				return cleanCities.get(0);
			else
				return null;
		}
		
		private String getCountry(JSONObject postObj, String city) {
			String cityCountry = null;
			
			if (city != null) {
				List<String> cityCountries = this.cityCountryMapGazetteer.getIds(city);
				if (cityCountries != null && !cityCountries.isEmpty())
					cityCountry = cityCountries.get(0);
			}
			
			if (!postObj.has("metadata") ||
					!postObj.getJSONObject("metadata").has("location") ||
					!postObj.getJSONObject("metadata").getJSONObject("location").has("country"))
				return cityCountry;
			
			String uncleanCountry = postObj.getJSONObject("metadata").getJSONObject("location").getString("country");
			List<String> cleanCountries = this.countryGazetteer.getIds(uncleanCountry);
			if (cleanCountries != null && !cleanCountries.isEmpty())
				return cleanCountries.get(0);
			else
				return cityCountry;
		}
		
		private boolean validLanguage(String city, String country) {
			if (languageFilter == null)
				return true;
			
			List<String> languages = new ArrayList<String>();
			if (city != null) {
				List<String> cityLanguages = this.locationLanguageMapGazetteer.getIds(city);
				if (cityLanguages != null)
					languages.addAll(cityLanguages);
			}
			
			if (country != null) {
				List<String> countryLanguages = this.locationLanguageMapGazetteer.getIds(country);
				if (countryLanguages != null)
					languages.addAll(this.locationLanguageMapGazetteer.getIds(country));
			}
			
			if (languages.contains(languageFilter))
				return true;
			else
				return false;
		}
	}

	public static class FeaturizeFacebookPostsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			this.outKey.set(key.toString());
			this.outValue.set(sum);
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("PROPERTIES_PATH", otherArgs[0]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HFeaturizeFacebookPosts");
		job.setJarByClass(HFeaturizeFacebookPosts.class);
		job.setMapperClass(FeaturizeFacebookPostsMapper.class);
		job.setCombinerClass(FeaturizeFacebookPostsReducer.class);
		job.setReducerClass(FeaturizeFacebookPostsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		if (otherArgs.length > 3)
			languageFilter = otherArgs[3];
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
