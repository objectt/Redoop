package com.boxple.redoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RedisReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
        
    	//key = new Text(key.toString() + "TEMP");
    	int keyCount = 0;
        
        for(IntWritable value: values){
        	keyCount+= value.get();
        }
        
        //System.out.println("RedisReducer::reduce key = " + key.toString());
        output.write(key, new Text(Integer.toString(keyCount)));
    }
}
