package unrest.facebook.hadoop;

import java.io.IOException;

import net.sf.json.JSONException;

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

import unrest.feature.UnrestFeature;
import unrest.feature.UnrestFeatureTotal;

/**
 * Takes in lines output from HFeaturizeFacebookPosts of the form:
 * 
 * [Date]	[Feature Type]	[Location]	[Feature Term]	[Count]
 * 
 * And outputs lines of the form:
 * 
 * [Date]	[Location]	[Count]
 * 
 * Right now, this is just used to compute total number of posts per date and location
 */
public class HAggregateFeaturesByDateLocation {
	private static UnrestFeature featureTypeFilter = new UnrestFeatureTotal();
	
	public static class AggregateFeaturesByDateLocationMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text key = new Text();
		private IntWritable value = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineParts = line.split("\\t");
			String date = lineParts[0];
			String featureType = lineParts[1];
			String location = lineParts[2];
			int count = Integer.parseInt(lineParts[4]);
			
			if (!featureType.equals(featureTypeFilter.getName()))
				return;
			
			this.key.set(date + "\t" + location);
			this.value.set(count);
			context.write(this.key, this.value);
		}
	}

	public static class AggregateFeaturesByDateLocationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws JSONException, IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
				
			this.outKey.set(key);
			this.outValue.set(sum);
			context.write(this.outKey, this.outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HAggregateFeaturesByDateLocation");
		job.setJarByClass(HAggregateFeaturesByDateLocation.class);
		job.setMapperClass(AggregateFeaturesByDateLocationMapper.class);
		job.setCombinerClass(AggregateFeaturesByDateLocationReducer.class);
		job.setReducerClass(AggregateFeaturesByDateLocationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
