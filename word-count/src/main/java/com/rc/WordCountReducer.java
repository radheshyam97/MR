package com.rc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Radheshyam
 * @see 16 Feb 2018
 * 
 * */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {	
		int sum=0;
		for (IntWritable value : values) {
			sum+=value.get();
		}
		context.write(new Text(key),new IntWritable(sum));
	}
}
