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

public class NodeWeight {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable output_key = new IntWritable();
		private Text output_value = new Text();

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			int count = 0;
			String input = values.toString();
			if (input.length() > 1) {
				String[] data = input.split("\t");
				for (String value: data) {
					output_key.set(Integer.parseInt(value));
					if (count != 0) {
						output_value.set("1	0");
					} else {
						output_value.set("0	1");
						count = 1;
					}
					context.write(output_key, output_value);
				}
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text output = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int inBound = 0;
			int outBound = 0;

			for (Text data : values) {
				String inOutBounds[] = data.toString().split("\t");
				inBound = inBound + Integer.parseInt(inOutBounds[0]);
				outBound = outBound + Integer.parseInt(inOutBounds[1]);
			}
			String finalData = inBound + "\t" + outBound;
			output.set(finalData);
			context.write(key, output);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "nodeWeight");

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setJarByClass(NodeWeight.class);
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
