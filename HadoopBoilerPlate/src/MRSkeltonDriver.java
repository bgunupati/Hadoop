/*
 * This will be base code for any project, all the projects more or less start with similar code base.
 * A default java project will gets created. And will hold only the java dependencies.
 * Hadoop dependencies are not added by default. They needs to be added by 
 * 		--> RIGHT CLICK ON THE PROJECT 
 * 		--> Build Path 
 * 		--> Configure Build Path... 
 * 		--> In Libraries TAB 
 * 		--> Click Add External JARs..
 * 		--> Navigate to Hadoop installation Path (in cloudera image it will be /usr/lib/Hadoop/)
 *  	--> Select All JAR Files and add them to project.
 *  	--> Do the same for Mapper and Reducer Jar file in the location (/usr/lib/hadoop-0.20-mapreduce/hadoop.core.xx.jar
 *  
 *  
 *  
 * POINTS TO NOTE :
 * 1) By default three classes are required in the project 
 * MRSkeletonDriver - this will be the point of entry into the application (In other words this holds the main method)
 * MRMapper 
 * MRReducer
 */

// *************************THIS CODE IS NOT COMPILING***********************************//

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRSkeltonDriver {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		// Context object - Mapper writes the output to local file system, but
		// it very tedious to write output to all the datanodes. So
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
		Job job = new Job(conf, "MR Skeleton Program");

		// Specify Mapper Recuder driver classes
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		// Specify Driver Class
		job.setJarByClass(MRSkeltonDriver.class);

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
