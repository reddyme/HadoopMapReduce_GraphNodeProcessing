package org.com.Hadoop_project;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NodeToNodeFlow {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable output_key = new IntWritable();
		private IntWritable output_value = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input = value.toString();
			if (input.length() > 1) {
				String[] data = input.split("\t");
				output_key.set(Integer.parseInt(data[0]));
				output_value.set(Integer.parseInt(data[2]));
				context.write(output_key, output_value);
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable output = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable val : values) {
				count = count + val.get();
			}
			output.set(count);
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "nodeflow");

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setJarByClass(NodeToNodeFlow.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);


		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
