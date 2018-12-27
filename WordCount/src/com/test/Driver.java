package com.test;

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

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		System.out.println("### DRIVER CODE START ###");

		// Job config Details

//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf);
//		job.setJobName("WordCount");
//		job.setJarByClass(Driver.class);
//
//		// Mepper
//
//		job.setMapperClass(WCMapper.class);
//
//		// Reducer
//		job.setReducerClass(WCReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		Path inputPath = new Path(args[0]);
//		Path outputPath = new Path(args[1]);
//
//		FileInputFormat.addInputPath(job, inputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);
//
//		job.waitForCompletion(true);
		
		
		//Create Configuration object
		Configuration conf=new Configuration();
		
		//Create job object
		Job job=Job.getInstance(conf);
		
		//setting the job name
		job.setJobName("Job Name: word-count");
		
		//setting the driver class name
		job.setJarByClass(Driver.class);
		
		//Setting the reduce class name
		job.setReducerClass(WCReducer.class);
		
		//Setting the mapper class name
		job.setMapperClass(WCMapper.class);
		
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

		System.out.println("### DRIVER CODE END ###");

	}

}
