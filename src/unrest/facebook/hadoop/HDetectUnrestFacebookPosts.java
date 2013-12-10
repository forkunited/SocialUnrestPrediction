package unrest.facebook.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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

import unrest.detector.Detector;
import unrest.detector.DetectorBBN;
import unrest.util.UnrestProperties;
import ark.data.Gazetteer;
import ark.util.StringUtil;

/**
 * Takes in lines output from HFilterFacebookDataToPosts of the form:
 * 
 * [Facebook Page ID]	[Facebook Post Object]
 * 
 * And outputs lines for every date on which unrest is detected of the form:
 * 
 * ["City"|"Country"]	[City|Country]	[Date]	[Unrest post count]	[Unrest post data]
 * 
 */
public class HDetectUnrestFacebookPosts {	
	public static class DetectUnrestFacebookPostsMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		
		private DetectorBBN detector = new DetectorBBN();
		private UnrestProperties properties = new UnrestProperties();
		private StringUtil.StringTransform cleanFn = StringUtil.getDefaultCleanFn();
		private Gazetteer cityGazetteer = new Gazetteer("City", this.properties.getCityGazetteerPath());
		private Gazetteer countryGazetteer = new Gazetteer("Country", this.properties.getCountryGazetteerPath());
		private Gazetteer cityCountryMapGazetteer = new Gazetteer("CityCountryMap", this.properties.getCityCountryMapGazetteerPath());
		private SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss+SSSS");
		private SimpleDateFormat outputDateFormat = new SimpleDateFormat("MM-dd-yyyy");
		private Calendar date = Calendar.getInstance();
		
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
			
			if (city == null && country == null)
				return;
			
			try {
				this.date.setTime(this.inputDateFormat.parse(lineObj.getString("date")));
			} catch (ParseException e) {
				return;
			}
			
			String message = lineObj.getString("message");
			
			Detector.Prediction prediction = this.detector.getPrediction(this.cleanFn.transform(message), this.date);
			if (prediction == null)
				return;
			
			String predictionLocation = prediction.getLocation();
			String predictionCity = city;
			String predictionCountry = country;
			if (predictionLocation != null) {
				if (this.cityGazetteer.contains(predictionLocation))
					predictionCity = predictionLocation;
				
				List<String> predictionCountries = this.cityCountryMapGazetteer.getIds(predictionLocation);
				if (predictionCountries != null && predictionCountries.size() > 0)
					predictionCountry = predictionCountries.get(0);
				else if (this.countryGazetteer.contains(predictionLocation))
					predictionCountry = predictionLocation;
			}
			
			Date predictionDate = prediction.getMinTime().getTime();
			
			if (predictionCity != null) {
				this.key.set("city\t" +
							predictionCity + "\t" + 
							this.outputDateFormat.format(predictionDate));
				this.value.set("1\t" + line);
				context.write(this.key, this.value);
			}
			
			if (predictionCountry != null) {
				this.key.set("country\t" +
						predictionCountry + "\t" + 
						this.outputDateFormat.format(predictionDate));
				this.value.set("1\t" + line);
				context.write(this.key, this.value);
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
		
	}

	public static class DetectUnrestFacebookPostsReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int postCount = 0;
			StringBuilder posts = new StringBuilder();
			for (Text value : values) {
				String[] valueParts = value.toString().split("\\t");
				postCount += Integer.parseInt(valueParts[0]);
				posts.append(valueParts[1]).append("\t");
			}

			this.outKey.set(key);
			this.outValue.set(postCount + "\t" + posts.toString().trim());
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HDetectUnrestFacebookPosts");
		job.setJarByClass(HDetectUnrestFacebookPosts.class);
		job.setMapperClass(DetectUnrestFacebookPostsMapper.class);
		job.setCombinerClass(DetectUnrestFacebookPostsReducer.class);
		job.setReducerClass(DetectUnrestFacebookPostsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
