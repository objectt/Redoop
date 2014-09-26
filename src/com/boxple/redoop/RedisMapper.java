package com.boxple.redoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RedisMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	//private Text out = new Text();
	private final IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Text key, Text value, Context contex) 
			throws IOException, InterruptedException {
		
		//out.set(key);
		contex.write(key, one);
	}
}
