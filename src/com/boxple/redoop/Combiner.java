package com.boxple.redoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<DateWordPair, IntWritable, DateWordPair, IntWritable> {

	private IntWritable keyCount = new IntWritable();
	private int intCount = 0;
	
    @Override
    public void reduce(DateWordPair key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
            	       
        for(IntWritable value: values){
        	intCount += value.get();
        }        
    	keyCount.set(intCount);
        
        output.write(key, keyCount);
    }
}
