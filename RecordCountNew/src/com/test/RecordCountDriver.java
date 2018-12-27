package com.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RecordCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println("START");

		try {
			// System.exit(ToolRunner.run(new RecordCountDriver(), args));
			ToolRunner.run(new RecordCountDriver(), args);
		} catch (Exception ex) {

			System.out.println("EX occured");
			ex.printStackTrace();
		} catch (Throwable t) {

			System.out.println("Th occured");
			t.printStackTrace();

		}
		System.out.println("END");
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setMapperClass(RecordCountMapper.class);
		job.setReducerClass(LongSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		Path inputPath = new Path("/home/radhe/Documents/hadoop_run/data_sets/NYSE_20181224.csv");
		Path outputPath = new Path("/home/radhe/Documents/hadoop_run/out/out1");
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	private static class RecordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		protected void map(LongWritable key, Text value,  Context context)
				throws IOException, InterruptedException {

			try {
				System.out.println("IN Mapper");
				System.out.println("key: " + key);
				System.out.println("Value: " + value.toString());

				context.write(new Text("count"), new LongWritable(1));
			} catch (Exception e) {
				System.out.println("EX occured");
				e.printStackTrace();
			} catch (Throwable e) {
				System.out.println("TH occured");
				e.printStackTrace();
			}

		}

	}

}
