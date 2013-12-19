package unrest.facebook.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import unrest.facebook.AggregateDateLocationMap;
import unrest.facebook.AggregateTermMap;
import unrest.util.LocationLanguageMap;
import unrest.util.UnrestProperties;


/**
 * Takes in lines output from HFeaturizeFacebookPosts where line i is of the form:
 * 
 * [Date i]	[Feature Type i]	[Location i]	[Feature Term i]	[Count i]
 * 
 * And outputs lines of the form:
 * 
 * [Feature Type i]	[Date j]	[Location j]	[[Feature Term j_1]:f([j_1]) [Feature Term j_2]:f([j_2]) ... [Feature Term j_n]:f([j_n])]
 *
 * Where [Date j_k] and [Location j_k] are the same across all k for k=1 to n.
 * 
 * The function f([x]) is defined as:
 * 
 * f([x]) = 2                        if (([Count x]/Total([x]))-Mean([x]))/SD([x]) > 2
 * or     = 1                        if [Count x] > 0
 * or     undefined (isn't computed) otherwise
 *
 * Total([x]) is the number of posts at ([Date x],[Location x]) (taken from output of HAggregateFeaturesByDateLocation)
 *
 * Mean([x]) is average of [Count x_i]/Total([x]) over all date-location pairs for a given feature term
 * SD([x]) is the standard deviation (defined similarly to Mean([x])
 * 
 * Mean and SD values are taken from the output of unrest.facebook.FeatureTermAggregateSplitter.
 * 
 * The output of HConstructTrainingData can be used as input for training the social unrest prediction model.
 */
public class HConstructTrainingData {
	public static class ConstructTrainingDataMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		
		private UnrestProperties properties;
		private LocationLanguageMap languageMap;
		private AggregateDateLocationMap dateLocationPostTotals;

		private Map<String, AggregateTermMap> aggregates = new HashMap<String, AggregateTermMap>();

		public void setup(Context context) {
			this.properties = new UnrestProperties(true, context.getConfiguration().get("PROPERTIES_PATH"));
			this.languageMap = new LocationLanguageMap(this.properties);
			this.dateLocationPostTotals = new AggregateDateLocationMap(this.properties.getFacebookPostDateLocationTotalsPath());
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineParts = value.toString().split("\\t");
			
			String date = lineParts[0];
			String featureType = lineParts[1];
			String location = lineParts[2];
			String featureTerm = lineParts[3];
			int count = Integer.parseInt(lineParts[4]);
			String language = this.languageMap.getLanguage(location);
			if (!this.aggregates.containsKey(language))
				this.aggregates.put(language, new AggregateTermMap(language, this.properties));
			AggregateTermMap aggregate = this.aggregates.get(language);
			if (!aggregate.hasFeature(featureType, featureTerm))
				return;
			
			double mean = aggregate.getMean(featureType, featureTerm);
			double sd = aggregate.getSD(featureType, featureTerm);
			
			double scaleValue = (((double)count)/this.dateLocationPostTotals.getAggregate(date, location) - mean)/sd;
			double termValue = 1.0;
			if (scaleValue > 2)
				termValue = 2.0;
			
			this.key.set(featureType + "\t" + date + "\t" + location);
			this.value.set(featureTerm + ":" + termValue);
			context.write(this.key, this.value);
		}
	}

	public static class ConstructTrainingDataReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws JSONException, IOException, InterruptedException {
			StringBuilder valuesStr = new StringBuilder();
			for (Text value : values) {
				valuesStr.append(value.toString()).append(" ");
			}
			if (valuesStr.length() == 0)
				return;
			valuesStr.delete(valuesStr.length()-1, valuesStr.length());
			
			this.outKey.set(key);
			this.outValue.set(valuesStr.toString());
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("PROPERTIES_PATH", otherArgs[0]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HConstructTrainingData");
		job.setJarByClass(HConstructTrainingData.class);
		job.setMapperClass(ConstructTrainingDataMapper.class);
		job.setCombinerClass(ConstructTrainingDataReducer.class);
		job.setReducerClass(ConstructTrainingDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
