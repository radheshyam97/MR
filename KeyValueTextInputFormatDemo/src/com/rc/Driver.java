package com.rc;

 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", args[2]);
 
		job.setJarByClass(Driver.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
  		 
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
