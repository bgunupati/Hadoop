package com.bgunupati.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxTemp {

	public static class MinMaxTempMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String inputTemp = value.toString();

			String datevalue = inputTemp.substring(7, 14).trim();
			String maxTemp = inputTemp.substring(39, 45).trim();
			String minTemp = inputTemp.substring(47, 53).trim();

			context.write(new Text(datevalue), new Text(maxTemp
					+ "," + minTemp));

		}
	}

	public static class MinMaxTempReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text minMaxT : values) {
				String minMax = minMaxT.toString();
				if (minMax.contains(",")) {
					if ((Float.valueOf((minMax.split(",")[0])) > 15)
							&& (Float.valueOf((minMax.split(",")[1])) < 10)) {
						context.write(key, new Text((minMax.split(",")[0])
								+ " " + (minMax.split(",")[1])));
					}
				}
			}

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 2) {
			System.out.println("Please provide input and output directories");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Map Reduce Max Temperature");

		job.setJarByClass(MinMaxTemp.class);

		job.setMapperClass(MinMaxTempMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MinMaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
