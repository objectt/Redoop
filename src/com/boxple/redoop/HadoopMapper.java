package com.boxple.redoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	//private Text out = new Text();	
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context contex) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, ",");

		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			contex.write(word, one);
		}
	}
} 
