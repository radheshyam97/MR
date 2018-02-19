package com.rc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KVReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Text name = null;
		for (Text text : values) {
			name = text;

		}
		context.write(key, name);

	}
}
