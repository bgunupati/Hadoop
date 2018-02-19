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

public class AlphabetsCount {

	public static class AlphabetMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String alphabetsString = value.toString();
			StringTokenizer tString = new StringTokenizer(alphabetsString);
			while(tString.hasMoreTokens())
			{
				context.write(new Text(String.valueOf(tString.nextToken().length())), new IntWritable(1));
			}
		}
	}
	
	public static class AlphabetReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int aCount = 0;
			for(IntWritable value : values)
			{
				aCount += value.get();
			}
			context.write(key, new IntWritable(aCount));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length <2)
		{
			System.out.println("Please provide input and output directories");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MR Alphabets count program");
		
		job.setJarByClass(AlphabetsCount.class);
		
		job.setMapperClass(AlphabetMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(AlphabetReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}

}
