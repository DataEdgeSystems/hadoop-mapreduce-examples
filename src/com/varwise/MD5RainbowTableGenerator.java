package com.varwise;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Simple Hadoop MapReduce job that calculates MD5 hashes for every world of input.
 * Then it makes reversed table of mappings: md5_hash -> word
 *
 */
public class MD5RainbowTableGenerator {

	public static enum Count {
		Length0, Length1, Length2, Length3, Length4, Length5
	}

	public static enum SetCounters {
		NumFlushBuffer
	}

	public static String getHashForId(String id) {

		byte[] theTextToDigestAsBytes = id.getBytes();
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");

			md.update(theTextToDigestAsBytes);

			byte[] digest = md.digest();

			Formatter formatter = new Formatter();
			for (byte b : digest) {
				formatter.format("%02x", b);
			}
			return formatter.toString();

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return "-1";
		}

	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text val = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				val.set(MD5RainbowTableGenerator.getHashForId(word.toString()));
				context.write(val, word);
			}
		}

	}

	public static class MapDistinct extends Mapper<LongWritable, Text, Text, Text> {
		private Text emptyText = new Text();

		private static final int BUFFER_SIZE = 10000;

		private Set<String> set;

		@Override
		public void setup(Context context) {
			set = new HashSet<String>();
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			writeSet(set, context);
		}

		public void writeSet(Set<String> set, Context ctx) throws IOException, InterruptedException {
			for (String val : set) {
				ctx.write(new Text(val), emptyText);
			}
			ctx.getCounter(SetCounters.NumFlushBuffer).increment(1);
			set.clear();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				set.add(tokenizer.nextToken());
				if (set.size() > BUFFER_SIZE) {
					writeSet(set, context);
				}
			}
		}

	}

	public static class ReduceDistinct extends Reducer<Text, Text, Text, Text> {
		private Text emptyText = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			context.write(key, emptyText);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Set<String> set = new HashSet<String>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text val : values) {
				set.add(val.toString());
			}

			for (String val : set) {
				sb.append(val).append(" ");
			}

			context.getCounter(Count.values()[set.size()]).increment(1);

			context.write(key, new Text(sb.toString().trim()));

			set.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job2 = new Job(conf, "hashmd5dist");
		job2.setJarByClass(MD5RainbowTableGenerator.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(MapDistinct.class);
		job2.setReducerClass(ReduceDistinct.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setNumReduceTasks(400);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "-distinct"));

		job2.waitForCompletion(true);

		Job job = new Job(conf, "hashmd5");
		job.setJarByClass(MD5RainbowTableGenerator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(400);

		FileInputFormat.addInputPath(job, new Path(args[1] + "-distinct"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}