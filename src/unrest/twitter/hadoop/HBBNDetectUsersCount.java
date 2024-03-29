package unrest.twitter.hadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import unrest.detector.Detector;
import unrest.detector.DetectorBBN;
import unrest.twitter.JsonTweet;
import unrest.util.UnrestProperties;
import ark.util.StringUtil;

public class HBBNDetectUsersCount {
	
	public static class BBNDetectUsersCountMapper extends Mapper<Object, Text, LongWritable, IntWritable> {
		private LongWritable userId;
		private IntWritable one;
		private StringUtil.StringTransform cleanFn;
		private DetectorBBN unrestDetector;
		private Calendar tweetDate;
		
		public void setup(Context context) {
			this.userId = new LongWritable(0);
			this.one = new IntWritable(1);
			this.cleanFn = StringUtil.getDefaultCleanFn();
			this.unrestDetector = new DetectorBBN(new UnrestProperties(true, context.getConfiguration().get("PROPERTIES_PATH")));
			this.tweetDate = Calendar.getInstance();
		}
		
		/*
		 * Skip badly gzip'd files
		 */
		public void run(Context context) throws InterruptedException {
			try {
				setup(context);
				this.tweetDate.setTimeZone(TimeZone.getTimeZone("America/New_York"));
				while (context.nextKeyValue()) {
					map(context.getCurrentKey(), context.getCurrentValue(),
							context);
				}
				cleanup(context);
			} catch (Exception e) {

			}
		}

		public void map(Object key, Text value, Context context) {

			String line = value.toString();
			JsonTweet tweet = null;
			
			try {
				tweet = JsonTweet.readJsonTweet(line);
				
				if (tweet != null) {
					String tweetText = this.cleanFn.transform(tweet.getText());
					
					this.tweetDate.setTime(tweet.getDate());
					Detector.Prediction prediction = this.unrestDetector.getPrediction(tweetText, this.tweetDate);
					if (prediction != null) {
						this.userId.set(tweet.getUserId());
						context.write(this.userId, this.one);
					}
				}

			} catch (Exception e1) {
				
			}
		}
	}

	public static class BBNDetectUsersCountReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("PROPERTIES_PATH", otherArgs[0]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HBBNDetectUsersCount");
		job.setJarByClass(HBBNDetectUsers.class);
		job.setMapperClass(BBNDetectUsersCountMapper.class);
		job.setCombinerClass(BBNDetectUsersCountReducer.class);
		job.setReducerClass(BBNDetectUsersCountReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
