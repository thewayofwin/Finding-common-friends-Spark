package q4;


import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {

	public static class FriendMap extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text(); // type of output key
		private Text outValue = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split("\\t");
			if (data.length == 1)
				return;
			String userID = data[0].toString();
			String[] friends = data[1].split(",");

			// set tag "id	userID|"
			userID = "id" + userID;
			outValue.set(userID);
			

			if (friends.length == 0)
				return;
			for (String friend : friends) {
				
				outKey.set(friend);
				context.write(outKey, outValue);
			}

		}
	}

	public static class AgeMap extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text(); // type of output key
		private Text outValue = new Text(); // type of output value
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split(",");
			if (data.length != 10)
				return;
			
			String userID = data[0].toString();
			//String DoB = data[9];
			String[] DoB = data[9].toString().split("/");			
					
			Calendar now = Calendar.getInstance();
			int diff = now.get(Calendar.YEAR) - Integer.parseInt(DoB[2].toString().trim());
		//	int diff = 2016 - Integer.parseInt(DoB[2].toString());
			outKey.set(userID);
			// set tag "age	age"
			String age = "age" + diff ;
			outValue.set(age);
			context.write(outKey, outValue);
		}

	}

	public static class Reduce extends Reducer<Text, Text, IntWritable, IntWritable> { 
		
		//private IntWritable outKey = new IntWritable();
		//private IntWritable outValue = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String v = null, id = null, age = null;
			List<String> list = new ArrayList<>();
			for(Text value: values)
			{
				list.add(value.toString());
				
			}
			// ignore the format does not match 
			if(list.size() != 2) return;
			
			for(String value : list)
			{
				v = value.toString();
				if(v.startsWith("id")){
					int len1 = "id".length();
					age = v.substring(len1);
				}
				else if(v.startsWith("age")){
					int len2 = "age".length();
					id = v.substring(len2);
				}
			}
			context.write(new IntWritable(Integer.parseInt(id)), new IntWritable(Integer.parseInt(age)));
			
			
			
//			String[] id = new String[]{};
//			String age = null ;
//			int count = 0;
//			
//			for (Text value : values) {
//				String[] parts = value.toString().trim().split("|");
//				
//				for(String v : parts){
//					String[] part = v.split("\t");
//					if (part[0].equals("id")) {
//						id[count++] = part[1];
//					} else if (part[0].equals("age")) {
//						age = part[1];
//					}						
//				}
//							
//			}
//			for(String value : id){
//				// output id and age
//				outKey.set(Integer.parseInt(value));
//				outValue.set(Integer.parseInt(age));
//				context.write(outKey, outValue);
//			}
//			
		}
		
	}

	public static class MapperOfMaxAges extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split("\\t");
			if (data.length != 2)
				return;
			context.write(new IntWritable(Integer.parseInt(data[0])), new IntWritable(Integer.parseInt(data[1])));

		}
	}

	public static class ReducerOfMaxAges extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int max = 0;
			for (IntWritable value : values)
				max = Math.max(max, value.get());
			if (max == 0)
				return;
			context.write(key, new IntWritable(max));

		}
	}
	

	public static class MapperOfSort extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
			
		
		HashMap<Integer, Integer> info = new HashMap<Integer, Integer>();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] data = value.toString().split("\\t");
			
			if (data.length != 2)
				return;
			IntWritable outKey = new IntWritable(Integer.parseInt(data[1]));
			IntWritable outValue = new IntWritable(Integer.parseInt(data[0]));
			context.write(outKey, outValue);

		}
	}
	

	public static class CompkeySortComparator extends WritableComparator {
		protected CompkeySortComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable com1, WritableComparable com2) {
			IntWritable w1 = (IntWritable) com1;
			IntWritable w2 = (IntWritable) com2;

			int comparator = w1.compareTo(w2);
			// decreasing order by reverse the sign
			if (comparator != 0)
				comparator *= -1;
			return comparator;
		}
	}

	public static class ReducerOfSortAge extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			List<IntWritable> list = new ArrayList<>();
			for (IntWritable value : values) {

				list.add(value);
				context.write(value, key);

			}
		}
	}

	public static class MapperOfMaxAgeAddress extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split("\\t");
			if (arr.length != 2)
				return;
			context.write(new IntWritable(Integer.parseInt(arr[0])), new Text(arr[1]));
		}
	}

	public static class Mapper_UserData_Address extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] arr = value.toString().split(",");
			// Ignore the case
			if (arr.length != 10)
				return;
			IntWritable keys = new IntWritable(Integer.parseInt(arr[0]));
			Text values = new Text(arr[1] + " " + arr[2] + ", " + arr[3] + "," + arr[4] + "," + arr[5]);
			context.write(keys, values);
		}
	}

	public static class ReducerOfMaxAgeAddress extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		private Text result = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String str = null;
			List<String> list = new ArrayList<>();

			for (Text value : values) {
				str = value.toString();
				if (list.size() == 0)
					list.add(str);
				else {
					int index = str.length() < 6 ? 1 : 0;
					list.add(index, str);
				}
			}
			if (list.size() != 2)
				return;
			String info = list.get(0) + "#" + list.get(1);
			result.set(info);
			context.write(key, result);

		}
	}

	public static class MapperOfOutput extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NumberFormatException {

			String[] arr = value.toString().split("\\t");
			if (arr.length != 2)
				return;
			String[] d = arr[1].toString().split("#");
			IntWritable keys = new IntWritable(Integer.parseInt(d[1].trim()));
			Text values = new Text(arr[0] + "#" + d[0]);
			context.write(keys, values);

		}
	}

	public static class ReducerOfOutput extends Reducer<IntWritable, Text, IntWritable, Text> {
		static int top10Counter = 0;

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (top10Counter == 10)
				return;
			String[] arr;
			IntWritable intWritable = new IntWritable();

			for (Text v : values) {
				arr = v.toString().split("#");
				if (arr.length != 2) {
					intWritable.set(-1);
					context.write(intWritable, new Text(v.toString()));
					top10Counter++;
					continue;
				} else {
					intWritable.set(Integer.parseInt(arr[0]));
					context.write(intWritable, new Text(arr[1] + "\t" + String.valueOf(key.get())));
					top10Counter++;
					if (top10Counter == 10)
						return; // found the top 10 max age we need
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.println("Usage: ReduceSideJoin <in> <in> <out>");
			System.exit(2);
		}
		
		// chain job starts here
		Configuration conf = new Configuration();
		String[] argument = new GenericOptionsParser(conf, args).getRemainingArgs();
		// First job starts here
		String out1 = "/sxt150130/output/temp1";
		Job job1 = new Job(conf, "ReduceSideJoin job1");
		job1.setJarByClass(ReduceSideJoin.class);
		job1.setReducerClass(Reduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job1, new Path(argument[0]), TextInputFormat.class, FriendMap.class);
		MultipleInputs.addInputPath(job1, new Path(argument[1]), TextInputFormat.class, AgeMap.class);

		FileOutputFormat.setOutputPath(job1, new Path(out1));
		job1.waitForCompletion(true);
		
		// Second job starts here
		String out2 = "/sxt150130/output/temp2";
		Job job2 = new Job(conf, "ReduceSideJoin Job 2");
		job2.setJarByClass(ReduceSideJoin.class);
		job2.setMapperClass(MapperOfMaxAges.class);
		job2.setReducerClass(ReducerOfMaxAges.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path(out1));
		FileOutputFormat.setOutputPath(job2, new Path(out2));
		job2.waitForCompletion(true);
		
		// Third job starts here
		String out3 = "/sxt150130/output/temp3";
		Job job3 = new Job(conf, "ReduceSideJoin Job3");
		job3.setJarByClass(ReduceSideJoin.class);
		job3.setMapperClass(MapperOfSort.class);
		job3.setReducerClass(ReducerOfSortAge.class);
		job3.setSortComparatorClass(CompkeySortComparator.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job3, new Path(out2));
		FileOutputFormat.setOutputPath(job3, new Path(out3));
		job3.waitForCompletion(true);
		
		// Fourth job starts here
		String out4 = "/sxt150130/output/temp4";
		Job job4 = new Job(conf, "ReduceSideJoin Job4");
		job4.setJarByClass(ReduceSideJoin.class);
		job4.setReducerClass(ReducerOfMaxAgeAddress.class);
		job4.setOutputKeyClass(IntWritable.class);
		job4.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job4, new Path(out2), TextInputFormat.class, MapperOfMaxAgeAddress.class);
		MultipleInputs.addInputPath(job4, new Path(argument[1]), TextInputFormat.class, Mapper_UserData_Address.class);
		FileOutputFormat.setOutputPath(job4, new Path(out4));
		job4.waitForCompletion(true);

		// Fifth job starts here
		Job job5 = new Job(conf, "ReduceSideJoin Job5");
		job5.setJarByClass(ReduceSideJoin.class);
		job5.setMapperClass(MapperOfOutput.class);
		job5.setReducerClass(ReducerOfOutput.class);
		job5.setSortComparatorClass(CompkeySortComparator.class);
		job5.setOutputKeyClass(IntWritable.class);
		job5.setOutputValueClass(Text.class);
		job5.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job5, new Path(out4));
		FileOutputFormat.setOutputPath(job5, new Path(argument[2]));
		//job5.waitForCompletion(true);
		
		
		boolean success = job5.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReduceSideJoin(), args);
		System.exit(exitCode);
	}

}
