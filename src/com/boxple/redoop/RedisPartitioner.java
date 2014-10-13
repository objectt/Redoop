package com.boxple.redoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RedisPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		System.out.println("numReduceTasks = " + numReduceTasks);
			
		String hashKey = key.toString();
		
		if(numReduceTasks == 0)
			return 0;
		
		if(hashKey.length() < 5)
			return 0;
		else
			return 1 % numReduceTasks;
		
		//return numReduceTasks;
	}
}
