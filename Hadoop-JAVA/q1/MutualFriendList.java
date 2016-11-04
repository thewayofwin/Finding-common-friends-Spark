package q1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriendList {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text(); // type of output key
		private Text outValue = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split("\\t");
			if (data.length == 1)
				return;

			String userID = data[0];
			String friendList = data[1];
			outValue.set(friendList);

			String[] friends = friendList.split(",");
			for (String friend : friends) {
				String pair = "";
				if (Integer.valueOf(userID) < Integer.valueOf(friend)) {
					pair = userID + "," + friend;
				} else {
					pair = friend + "," + userID;
				}
				outKey.set(pair);
				if (outKey.toString().equals("0,1") || outKey.toString().equals("20,28193")
						|| outKey.toString().equals("1,29826") || outKey.toString().equals("6222,19272")
						|| outKey.toString().equals("28041,28056")) {
					context.write(outKey, outValue);
				}

			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		private String[] findFriend(String friendList1, String friendList2) {

			// find common friends
			HashSet<String> s1 = new HashSet<>();
			HashSet<String> s2 = new HashSet<>();

			for (String friend : friendList1.split(",")) {
				s1.add(friend);
			}
			for (String friend : friendList2.split(",")) {
				s2.add(friend);
			}

			s1.retainAll(s2);
			String[] common = new String[s1.size()];
			
			int i = 0;
			for (String friend : s1) {
				common[i++] = friend;
			}

			return common;
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] twoList = new String[2];
			int count = 0;
			for (Text value : values) {
				twoList[count++] = value.toString();
			}

			String[] mutualFriend = findFriend(twoList[0], twoList[1]);
			String output = "";
			if (mutualFriend.length == 0){
				output = "None";
				result.set(output);
				}
			else {
				Arrays.sort(mutualFriend);

				for (int i = 0; i < mutualFriend.length; i++) {
					output += mutualFriend[i] + ",";
				}
				output = output.substring(0, output.length() - 1);
				result.set(output);

			}
			context.write(key, result);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriendList <in> <out>");
			System.exit(2);
		}
		// create a job with name "MutualFriendList"
		Job job = new Job(conf, "MutualFriendList");
		job.setJarByClass(MutualFriendList.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
