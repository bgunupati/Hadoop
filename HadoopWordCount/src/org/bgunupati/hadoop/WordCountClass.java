package org.bgunupati.hadoop;
/* This is word count program with the boiler plate code.
 * The PROJECT is converted into Maven project and required dependencies are added in POM.XML file.
 * IMPORTANT : import org.apache.hadoop.mapred.FileInputFormat; -- This import format is old version and obsolete.
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountClass {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		// Context object - Mapper writes the output to local file system, but
		// it very tedious to write output to all the data nodes. So
		// context object is used by framework to write output to local system.
		public void map(LongWritable key, Text value, Context context) {
			// Here MapSide business logic goes here. Data Preparation logic
			// final key value will be written to context object ex
			// context.write(mynewk, mynewv)
		}
	}

	public static class MyReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		// Reduce method will take data in key value collection due to Shuffle
		// and Sort Phase of framework.
		public void recude(Text Key, Iterable<Text> values, Context context) {
			// Reduce side business log or Actaul business logic
			// final write key value to context object.
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Driver Logic

		// Step 1 : Configuration object
		// Because job requires
		Configuration conf = new Configuration();
		// Step 2: Create a Job
		Job job = new Job(conf, "MR Word Count Program");
		
		// Specify Mapper Recuder driver classes
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		// Specify Driver Class
		job.setJarByClass(WordCountClass.class);

		// MapOutPutKey, MapOutPutValue, ReducerInputKey, ReduderInputValue

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// input format output format
		// Text Input format

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
