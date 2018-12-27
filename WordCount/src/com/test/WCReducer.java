package com.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("******* Reducer START *******");

		System.out.println("### Key: " + key);
		System.out.println("### Itr: " + values);

		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}

		context.write(key, new IntWritable(count));

		System.out.println("******* Reducer END *******");

	}

}
