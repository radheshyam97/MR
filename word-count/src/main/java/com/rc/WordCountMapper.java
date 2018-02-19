package com.rc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Radheshyam
 * @see 16 Feb 2018
 * 
 * */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("key: "+key);
		
		String[] split = value.toString().split(",");
		
		for (String word : split) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
