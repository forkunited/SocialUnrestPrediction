package unrest.facebook.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import unrest.util.UnrestProperties;

/**
 * Takes in a single Facebook data JSON object per line (from scraper), and outputs lines of the form:
 * 
 * [Facebook Page ID]	[Facebook Post Object]
 * 
 * There can be several post objects for a given page id (resulting in several output lines).
 * A post object consists of data for a single Facebook post (originally from a "FEED" Facebook scraper
 * JSON object), along with meta-data describing the page on which the post was posted (originally from
 * a "MAIN" Facebook scraper JSON object.
 */
public class HFilterFacebookDataToPosts {
	
	public static class FilterFacebookDataToPostsMapper extends Mapper<Object, Text, Text, Text> {
		private Text key = new Text();
		private Text value = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			JSONObject lineObj = JSONObject.fromObject(line);
			if (!lineObj.has("type") || !lineObj.has("id") || !lineObj.has("response"))
				return;
			String type = lineObj.getString("type");
			String id = lineObj.getString("id");
			JSONObject responseObj = lineObj.getJSONObject("response");
			
			this.key.set(id);
			
			if (type.equals("FEED")) {
				if (!responseObj.has("data"))
					return;
				JSONArray responseData = responseObj.getJSONArray("data");
				for (int i = 0; i < responseData.size(); i++) {
					JSONObject datumObj = responseData.getJSONObject(i);
					if (!datumObj.has("created_time") || !datumObj.has("message"))
						continue;
					String date = datumObj.getString("created_time");
					String message = datumObj.getString("message");
					
					JSONObject outputObj = new JSONObject();
					outputObj.put("date", date);
					outputObj.put("message", message);
					outputObj.put("id", id);
					outputObj.put("type", "FEED");
					
					this.value.set(outputObj.toString());
					context.write(this.key, this.value);
				}
			} else if (type.equals("MAIN")) {
				if (!responseObj.has("location") || !responseObj.has("name"))
					return;
				JSONObject locationObj = responseObj.getJSONObject("location");
				String name = responseObj.getString("name");
				
				JSONObject outputObj = new JSONObject();
				outputObj.put("location", locationObj);
				outputObj.put("name", name);
				outputObj.put("type", "MAIN");
				
				this.value.set(outputObj.toString());
				context.write(this.key, this.value);
			}
		}
	}

	public static class FilterFacebookDataToPostsReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws JSONException, IOException, InterruptedException {
			JSONObject mainObj = null;
			List<JSONObject> feedObjs = new ArrayList<JSONObject>();
			
			for (Text value : values) {
				try {
					JSONObject valueObj = JSONObject.fromObject(value.toString());
					if (valueObj.getString("type").equals("FEED"))
						feedObjs.add(valueObj);
					else {
						this.outKey.set(key);
						this.outValue.set(valueObj.toString());
						
						if (valueObj.getString("type").equals("MAIN")) {
							mainObj = valueObj;
							//if (context.getTaskAttemptID().getTaskID().getTaskType().equals(TaskType.MAP))
							//	context.write(this.outKey, this.outValue); // Only output if combiner
						} else {
							context.write(this.outKey, this.outValue);
						}
					}
				} catch (JSONException e) {
					throw new JSONException("FAILED: " + value.toString());
				}
			}
			
			for (JSONObject feedObj : feedObjs) {
				if (mainObj != null) {
					feedObj.put("type", "POST");
					feedObj.put("metadata", mainObj);
				}
				
				this.outKey.set(key);
				this.outValue.set(feedObj.toString());
				
				if (mainObj != null)// || context.getTaskAttemptID().getTaskID().getTaskType().equals(TaskType.MAP))
					context.write(this.outKey, this.outValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "HFilterFacebookDataToPosts");
		job.setJarByClass(HFilterFacebookDataToPosts.class);
		job.setMapperClass(FilterFacebookDataToPostsMapper.class);
		//job.setCombinerClass(FilterFacebookDataToPostsReducer.class);
		job.setReducerClass(FilterFacebookDataToPostsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		UnrestProperties.PROPERTIES_PATH = otherArgs[0];
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

