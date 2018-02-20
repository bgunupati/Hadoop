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

public class MaxTemp {

	public static class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String inputTemp = value.toString();
			String[] tempDetails = inputTemp.split(" ");
			if(tempDetails.length > 1)
			{
				context.write(new Text(tempDetails[0]), new IntWritable(Integer.valueOf(tempDetails[1])));
			}
		}
	}
	
	public static class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int MaxTemp = 0;
			
			for(IntWritable temp : values)
			{
				if(MaxTemp <  temp.get())
				{
					MaxTemp = temp.get();
				}
			}
			context.write(key, new IntWritable(MaxTemp));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2)
		{
			System.out.println("Please provide input and output directories");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf,"Map Reduce Max Temperature");
		
		job.setJarByClass(MaxTemp.class);
		
		job.setMapperClass(MaxTempMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
