package com.bgunupati.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCountClass {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String inputString = value.toString();
			StringTokenizer tokenString = new StringTokenizer(inputString);
			Text word = new Text();
			while (tokenString.hasMoreTokens()) {
				word.set(tokenString.nextToken());
				context.write(word, new IntWritable(1));
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int wCount = 0;
			for (IntWritable value : values) {
				wCount += value.get();
			}
			context.write(key, new IntWritable(wCount));
		}

	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 2) {
			System.out.println("Provide input and output directories");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "MR Word Count Program");

		job.setJarByClass(WordCountClass.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
