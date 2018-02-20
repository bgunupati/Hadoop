package com.bgunupati.hadoop;

import java.io.IOException;

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

public class CountSubPatents {

	public static class PatentCounterMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String input = value.toString();
			String[] patentDetails = input.split(" ");
			if(patentDetails.length > 1)
			{
				context.write(new Text(patentDetails[0]), new Text(patentDetails[1]));
			}
		}
	}
	
	
	public static class PatentCounterReducer extends Reducer<Text,Text, Text, IntWritable>
	{
		public void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int CountOfPatents = 0;
			for(Text subPatents : values)
			{
				CountOfPatents++;
			}
			context.write(key, new IntWritable(CountOfPatents));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {		
		if(args.length < 2)
		{
			System.out.println("Please provide input and output directories");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Map Reduce for Sub Patent Count");
		
		job.setJarByClass(CountSubPatents.class);
		
		job.setMapperClass(PatentCounterMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		job.setReducerClass(PatentCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
