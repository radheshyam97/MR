package com.rc;

 import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
/**
 * @author Radheshyam
 * @see 16 Feb 2018
 * 
 * */
public class Driver {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//Create Configuration object
		Configuration conf=new Configuration();
		
		//Create job object
		Job job=Job.getInstance(conf);
		
		//setting the job name
		job.setJobName("Job Name: word-count");
		
		//setting the driver class name
		job.setJarByClass(Driver.class);
		
		//Setting the reduce class name
		job.setReducerClass(WordCountReducer.class);
		
		//Setting the mapper class name
		job.setMapperClass(WordCountMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		//Setting the input format class name
		 job.setInputFormatClass(TextInputFormat.class);
		
		//Setting the output format class name 
		 job.setOutputFormatClass(TextOutputFormat.class);
		
		  // Set input and output file paths
        FileInputFormat.addInputPath(job, new Path(args[0])) ;
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
 		job.waitForCompletion(true);
		
	}
}
