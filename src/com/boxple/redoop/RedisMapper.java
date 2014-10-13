package com.boxple.redoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RedisMapper extends Mapper<Text, Text, Text, IntWritable>{

	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	private final RedisPreCombiner<Text, IntWritable> combiner = new RedisPreCombiner<Text, IntWritable>(
		new CombiningFunction<IntWritable>() {
			@Override
			public IntWritable combine(IntWritable value1, IntWritable value2) {
				value1.set(value1.get() + value2.get());
				return value1;
			}
	});
	
	@Override
	public void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, ",");
		
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			//contex.write(word, one);
			combiner.write(word, one, context);
		}
		
		//System.out.println("map - " + key.toString());
		//contex.write(key, one);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context);
	}
} 