package com.boxple.redoop;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import redis.clients.jedis.Jedis;

public class RedisMapper extends Mapper<Text, Text, Text, IntWritable>{

	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Jedis jedisInstance;
	
	private final RedisPreCombiner<Text, IntWritable> combiner = new RedisPreCombiner<Text, IntWritable>(
		new CombiningFunction<IntWritable>() {
			@Override
			public IntWritable combine(IntWritable value1, IntWritable value2) {
				value1.set(value1.get() + value2.get());
				return value1;
			}
	});
	
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
    }
	
	@Override
	public void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] member = line.split(",");
		//StringTokenizer itr = new StringTokenizer(line, ",");
		
		word.set(member[2]);
		combiner.write(word, one, context);
		//context.write(word, one);
		
		word.set(member[3]);
		combiner.write(word, one, context);
		//context.write(word, one);
		
//		while (itr.hasMoreTokens()) {
//			word.set(itr.nextToken());
//			//context.write(word, one);
//			combiner.write(word, one, context);
//		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		combiner.flush(context);
	}
} 