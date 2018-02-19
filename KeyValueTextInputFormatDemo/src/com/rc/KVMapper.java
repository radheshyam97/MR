package com.rc;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVMapper extends Mapper<Text, Text, LongWritable, Text> {
    private static final Logger LOG = Logger.getLogger(KVMapper.class.getName());

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        LOG.log(Level.INFO, "MAPPER_KEY: ".concat(key.toString()).concat(" MAPPER_VALUE: ".concat(value.toString())));

		System.out.println("****Key: " + key);
		System.out.println("****Value: " + value);
		String name = value.toString().split("\t")[0];
		System.out.println("****name: " + name);
		context.write( new LongWritable(Long.parseLong(key.toString())) , new Text(name));

	}

}
