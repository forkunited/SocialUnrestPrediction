package unrest.facebook.hadoop;

import java.io.IOException;

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
import unrest.util.LocationLanguageMap;
import unrest.util.UnrestProperties;

/**
 * Takes in lines output from HFeaturizeFacebookPosts of the form:
 * 
 * [Date]	[Feature Type]	[Location]	[Feature Term]	[Count]
 * 
 * And outputs lines of the form:
 * 
 * [Language]	[Feature Type]	[Feature Term]	[Mean]	[Standard Deviation]	[Count]
 * 
 * [Mean] and [Standard Deviation] are computed across [Count] normalized by the total number of 
 * posts across each ([Date], [Location]).
 * 
 * This output can be run through unrest.facebook.FeatureAggregateSplitter to produce vocabulary
 * and mean/sd files for each language and feature.
 */
public class HAggregateFeaturesByTerm {
	public static class AggregateFeaturesByTermMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();
		
		private UnrestProperties properties = new UnrestProperties();
		private LocationLanguageMap languageMap = new LocationLanguageMap(properties);
		private AggregateDateLocationMap dateLocationPostTotals = new AggregateDateLocationMap(this.properties.getFacebookPostDateLocationTotalsPath());
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineParts = line.split("\\t");
			String date = lineParts[0];
			String featureType = lineParts[1];
			String location = lineParts[2];
			String featureTerm = lineParts[3];
			int count = Integer.parseInt(lineParts[4]);
			String language = this.languageMap.getLanguage(location);
			double dateLocationTotal = this.dateLocationPostTotals.getAggregate(date, location);
			
			if (language == null)
				return;
			
			this.key.set(language + "\t" + featureType + "\t" + featureTerm);
			this.value.set(count + "\t" + dateLocationTotal);
			context.write(this.key, this.value);
		}
	}

	public static class AggregateFeaturesByTermReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		private UnrestProperties properties = new UnrestProperties();
		private AggregateDateLocationMap dateLocationPostTotals = new AggregateDateLocationMap(this.properties.getFacebookPostDateLocationTotalsPath());
		public void reduce(Text key, Iterable<Text> values, Context context) throws JSONException, IOException, InterruptedException {
			int totalCount = 0;
			double totalFreq = 0;
			double totalSquaredFreq = 0;
			
			for (Text value : values) {
				String[] valueParts = value.toString().split("\\t");
				int count = Integer.parseInt(valueParts[0]);
				double dateLocationTotal = Double.parseDouble(valueParts[1]);
				double freq = count/dateLocationTotal;
				
				totalFreq += freq;
				totalSquaredFreq += freq*freq;
				totalCount += count;
			}
			
			int n = this.dateLocationPostTotals.getAggregateCount();
			double mean = totalFreq/n;
			double variance = (totalSquaredFreq-(totalFreq*totalFreq)/n)/(n-1);
			double sd = Math.sqrt(variance);
			
			this.outKey.set(key);
			this.outValue.set(mean + "\t" + sd + "\t" + totalCount);
			
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HAggregateFeaturesByTerm");
		job.setJarByClass(HAggregateFeaturesByTerm.class);
		job.setMapperClass(AggregateFeaturesByTermMapper.class);
		job.setReducerClass(AggregateFeaturesByTermReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
