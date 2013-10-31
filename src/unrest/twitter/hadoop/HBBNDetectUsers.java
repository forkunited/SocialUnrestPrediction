package unrest.twitter.hadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;


import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import ark.util.StringUtil;

public class HBBNDetectUsers {
	
	public static class BBNDetectUsersMapper extends Mapper<Object, Text, LongWritable, Text> {
		private LongWritable userId = new LongWritable(0);
		private Text predictionData = new Text();
		private StringUtil.StringTransform cleanFn = StringUtil.getDefaultCleanFn();
		private DetectorBBN unrestDetector = new DetectorBBN();
		private Calendar tweetDate = Calendar.getInstance();
		
		/*
		 * Skip badly gzip'd files
		 */
		public void run(Context context) throws InterruptedException {
			try {
				this.tweetDate.setTimeZone(TimeZone.getTimeZone("America/New_York"));
				setup(context);
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
						JSONObject data = new JSONObject();
						data.put("tweet", JSONObject.fromObject(line));
						data.put("prediction", prediction.toJSONObject());
						
						this.userId.set(tweet.getUserId());
						this.predictionData.set(data.toString());
						context.write(this.userId, this.predictionData);
					}
				}

			} catch (Exception e1) {
				
			}
		}
	}

	public static class BBNDetectUsersReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		private Text fullText = new Text();
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text text : values) {
				fullText.append(text.getBytes(), 0, text.getLength());
				fullText.append("\t".getBytes(), 0, 1);
			}
			
			context.write(key, this.fullText);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HBBNDetectUsers");
		job.setJarByClass(HBBNDetectUsers.class);
		job.setMapperClass(BBNDetectUsersMapper.class);
		job.setCombinerClass(BBNDetectUsersReducer.class);
		job.setReducerClass(BBNDetectUsersReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
