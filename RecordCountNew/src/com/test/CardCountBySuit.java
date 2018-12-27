package com.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CardCountBySuit extends Configured implements Tool {

	private static class CardCountBySuitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(final LongWritable key, final Text value, Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\\|");
			
			System.out.println(value);
				System.out.println("item: "+values[1]);
			context.write(new Text(values[1]), new LongWritable(1));
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {

		Job job = Job.getInstance(getConf());

		FileInputFormat.addInputPath(job, new Path("/home/radhe/Documents/dataset/data-master/cards/deckofcards.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/home/radhe/Documents/hadoop_run/out/out5"));

		job.setMapperClass(CardCountBySuitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Default no need to set
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(4);

		return job.waitForCompletion(true) ? 0 : 1;

	}
	public static void main(String[] args) throws Exception{
		
		System.out.println("START");
		
		ToolRunner.run(new CardCountBySuit(), args);
		System.out.println("END");

		
		
		
	}

}
