package com.hadoop.training;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


public class CustomPartitionerEx {

	final static Logger logger = Logger.getLogger(CustomPartitionerEx.class);
	public static class CustomMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			// Name,age,gender,marks
			String[] words = value.toString().split(",");
			int age = Integer.parseInt(words[1]);
			logger.info("Key:"+age+" Value:"+value);
			context.write(new IntWritable(age), value);
		}
	}
	
	public static class CustomReducer extends Reducer<IntWritable,Text,NullWritable,Text>{
		
		@Override
		public void reduce(IntWritable key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
			int maxMarks = Integer.MIN_VALUE;
			Text val = new Text();
			for(Text value:values){
				int marks = Integer.parseInt(value.toString().split(",")[1]);
				logger.info("Marks:"+marks);
				if(marks > maxMarks){
					maxMarks = marks;
					val = value;
				}
				logger.info("MxMarks:"+maxMarks+" Value:"+val.toString());
			}
			logger.info("Reducer:"+val);
			context.write(NullWritable.get(), val);
			
		}
		
	}
	
	public static class CustomPartitioner extends Partitioner<IntWritable,Text>{

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			if(key.get() < 15){
				return 0;
			}else if (key.get() >= 15 && key.get() <20 ){
				return 1;
			}else{
				return 2;
			}
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CustomPartitioner Example");
		job.setJarByClass(CustomPartitionerEx.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
