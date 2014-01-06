package unrest.facebook.hadoop;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

import unrest.feature.UnrestFeature;
import unrest.feature.UnrestFeatureConjunction;
import unrest.feature.UnrestFeatureFixedEffects;
import unrest.feature.UnrestFeatureFutureDate;
import unrest.feature.UnrestFeatureGazetteer;
import unrest.feature.UnrestFeatureUnigram;
import unrest.util.UnrestProperties;
import ark.data.Gazetteer;

/**
 * Takes in lines output from HFilterFacebookDataToPosts of the form:
 * 
 * [Facebook Page ID]	[Facebook Post Object]
 * 
 * And outputs lines of the form:
 * 
 * [Date].[Location]	[Sentence ID i]	{"[f_i0]":[v_i1], "[f_i1]":[v_i1],...}
 * 
 * Each line gives a JSON object storing values of features for the sentence.
 * JSON objects with Sentence ID "s_0" store values of features aggregated 
 * across all sentences for a given date and location (s_0 is a fake 
 * sentence). These features are aggregated across all sentences because it 
 * doesn't make sense to regularize them out by sentence in the sentence
 * regularization model.
 * 
 * The output can be fed into unrest.facebook.AggregateSRegFeatures to convert
 * to the proper format for Dani's sentence regularizing model.
 * 
 */
public class HFeaturizeFacebookPostSReg {
	private static String languageFilter = "es";
	private static boolean outputByCity = false;
	
	public static class FeaturizeFacebookPostSRegMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		
		private List<UnrestFeature> features;
		private List<UnrestFeature> sentenceFeatures;
		private UnrestProperties properties; 
		private Gazetteer cityGazetteer; 
		private Gazetteer countryGazetteer; 
		private Gazetteer cityCountryMapGazetteer; 
		private Gazetteer locationLanguageMapGazetteer;
		private SimpleDateFormat inputDateFormat; 
		private SimpleDateFormat outputDateFormat; 
		private Calendar date; 
		
		protected List<UnrestFeature> constructFeatures(UnrestProperties properties) {
			Gazetteer unrestTerms = new Gazetteer("UnrestTermLarge", properties.getUnrestTermLargeGazetteerPath());
			
			UnrestFeature unigram = new UnrestFeatureUnigram();
			UnrestFeature tom = new UnrestFeatureFutureDate(true);
			UnrestFeature hand = new UnrestFeatureGazetteer(unrestTerms);
			UnrestFeature handTom = new UnrestFeatureConjunction("handTom", hand, tom);
			UnrestFeature unigramTom = new UnrestFeatureConjunction("unigramTom", unigram, tom);
			UnrestFeature fixedEffects = new UnrestFeatureFixedEffects();
			
			List<UnrestFeature> features = new ArrayList<UnrestFeature>();
			
			features.add(hand);
			features.add(handTom);
			features.add(unigramTom);
			features.add(fixedEffects);
			
			return features;
		}
		
		protected List<UnrestFeature> constructSentenceFeatures(UnrestProperties properties) {
			UnrestFeature unigram = new UnrestFeatureUnigram();
			List<UnrestFeature> features = new ArrayList<UnrestFeature>();
			
			features.add(unigram);
			
			return features;
		}
		
		public void setup(Context context) {
			String propertiesPath = context.getConfiguration().get("PROPERTIES_PATH");
			this.properties = new UnrestProperties(true, propertiesPath);
			this.features = constructFeatures(this.properties);
			this.sentenceFeatures = constructSentenceFeatures(this.properties);
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
			
			String id = lineObj.getString("id");
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
			String location = null;
			if (outputByCity && city != null) {
				location = city;
			} else if (!outputByCity && country != null) {
				location = country;
			} else {
				return;
			}

			
			StringBuilder keyStrBuilder = new StringBuilder();
			keyStrBuilder = keyStrBuilder.append(this.outputDateFormat.format(this.date.getTime()));
			keyStrBuilder = keyStrBuilder.append(".");
			keyStrBuilder = keyStrBuilder.append(location);
			String keyStr = keyStrBuilder.toString();
			
			List<String> sentences = getSentences(message);
			
			// Compute sentence features
			for (int i = 0; i < sentences.size(); i++) {
				String sentenceId = "s_" + id + "_" + i;
				JSONObject sentenceObj = new JSONObject();
				for (UnrestFeature feature : this.sentenceFeatures) {
					String featureName = feature.getName();
					Map<String, Integer> featureValues = feature.compute(sentences.get(i), this.date, location);
					for (Entry<String, Integer> featureValue : featureValues.entrySet()) {
						if (featureValue.getKey().trim().length() == 0)
							continue;
						sentenceObj.put(featureName + "_" + featureValue.getKey(), featureValue.getValue());
					}
				}
				
				this.key.set(keyStr);
				this.value.set(sentenceId + "\t" + sentenceObj.toString());
				context.write(this.key, this.value);
			}
			
			JSONObject featuresObj = new JSONObject();
			// Compute rest of features
			for (UnrestFeature feature : this.features) {
				String featureName = feature.getName();
				Map<String, Integer> featureValues = feature.compute(message, this.date, location);
				for (Entry<String, Integer> featureValue : featureValues.entrySet()) {
					if (featureValue.getKey().trim().length() == 0)
						continue;
					
					featuresObj.put(featureName + "_" + featureValue.getKey(), featureValue.getValue());
				}
			}

			
			this.key.set(keyStr);
			this.value.set("s_0\t" + featuresObj.toString());
			context.write(this.key, this.value);
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
		
		private List<String> getSentences(String message) {
			Reader reader = new StringReader(message);
			DocumentPreprocessor dp = new DocumentPreprocessor(reader);

			List<String> sentenceList = new ArrayList<String>();
			Iterator<List<HasWord>> it = dp.iterator();
			while (it.hasNext()) {
			   StringBuilder sentenceSb = new StringBuilder();
			   List<HasWord> sentence = it.next();
			   for (HasWord token : sentence) {
			      if(sentenceSb.length()>1) {
			         sentenceSb.append(" ");
			      }
			      sentenceSb.append(token);
			   }
			   sentenceList.add(sentenceSb.toString());
			}
			
			return sentenceList;
		}
	}

	public static class FeaturizeFacebookPostSRegReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@SuppressWarnings("rawtypes")
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			JSONObject aggregateS0Obj = new JSONObject();
			for (Text value : values) {
				String[] valueParts = value.toString().split("\t");
				String sentenceId = valueParts[0];
				String valueObjStr = valueParts[1];
				
				if (!sentenceId.equals("s_0")) {
					this.outKey.set(key);
					this.outValue.set(value);
					context.write(this.outKey, this.outValue);
				} else {
					JSONObject valueObj = JSONObject.fromObject(valueObjStr);
					Set entries = valueObj.entrySet();
					for (Object o : entries) {
						Entry e = (Entry)o;
						String feature = e.getKey().toString();
						Integer featureValue = (Integer)e.getValue();
						
						if (!aggregateS0Obj.containsKey("s_0"))
							aggregateS0Obj.put(feature, featureValue);
						else
							aggregateS0Obj.put(feature, (Integer)aggregateS0Obj.get("s_0") + featureValue);
					}
				}
			}
			
			this.outKey.set(key);
			this.outValue.set("s_0\t" + aggregateS0Obj.toString());
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("PROPERTIES_PATH", otherArgs[0]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HFeaturizeFacebookPostsSReg");
		job.setJarByClass(HFeaturizeFacebookPostSReg.class);
		job.setMapperClass(FeaturizeFacebookPostSRegMapper.class);
		job.setCombinerClass(FeaturizeFacebookPostSRegReducer.class);
		job.setReducerClass(FeaturizeFacebookPostSRegReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		if (otherArgs.length > 3)
			languageFilter = otherArgs[3];
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
