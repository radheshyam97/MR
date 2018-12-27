package com.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

 
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("*** setup() Called ***");
 		super.setup(context);
	}
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		
		
		System.out.println("####key: "+key);
		
		String[] split = value.toString().split(" ");
		for (String word : split) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("*** cleanup() Called ***");
		super.cleanup(context);
	}
}
