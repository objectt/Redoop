package com.boxple.redoop;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.text.ParseException;

public class CombinerNode<KEY extends Writable, VALUE extends Writable> {
	
	  private Jedis cache;
	  private CombiningFunction < VALUE > combiningFunction;
	  private int minThreshold;
	  
	  private int taskId;
	  private String prefix = "MR:JOB:";
	  private String keyStr, valueStr;
	  
	  // Should fix these to match data types
	  private int valueInt;
	  //private Text outputKey = new Text();
	  private IntWritable outputValue = new IntWritable();
	  private DateWordPair outputDateWordPair = new DateWordPair();
	  
//	  public void setPort(int port){
//		  cache = new Jedis("127.0.0.1", port);
//		  cache.getClient().setTimeoutInfinite();
//		  cache.connect();
//	  }

	  public CombinerNode(CombiningFunction<VALUE> combiningFunction, int port, int minThreshold){
		  this.combiningFunction = combiningFunction;
		  this.minThreshold = minThreshold;
		  
		  cache = new Jedis("127.0.0.1", port);
		  cache.getClient().setTimeoutInfinite();
		  cache.connect();
	  }
	  
	  public void setMapperStart(int tid){
		  taskId = tid;
		  cache.set(prefix + taskId, "0");
		  //cache.incr(prefix + "NUM");
	  }
	  
	  public boolean isLastNodeMapper(){
		  return false;
	  }
	  
	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void write(KEY key, VALUE value, Mapper.Context context) 
			  throws InterruptedException, IOException {

		  key = WritableUtils.clone(key, context.getConfiguration());
		  value = WritableUtils.clone(value, context.getConfiguration());
		  		  
		  keyStr = key.toString();
		  valueStr = value.toString();
		  valueInt = Integer.parseInt(valueStr);	// Fix

		  if (combiningFunction != null){
			try {
				cache.incrBy(keyStr, valueInt);
				
//		        if (!cache.exists(keyStr)){
//		        	cache.set(keyStr, valueStr);
//		        } else {
//		        	//cache.set(key, combiningFunction.combine(lruCache.get(key), value));
//		        	cache.incrBy(keyStr, valueInt);
//		        }
			} catch(Exception ex){}
		  } else {
			context.write(key, value);
		  }
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void flush(Mapper.Context context) throws IOException, InterruptedException, ParseException {		  
		for(String key : cache.keys("*")){
			if(!(key.substring(0, prefix.length())).equals(prefix)){	
				valueInt = Integer.parseInt(cache.get(key));
				
				if(valueInt > minThreshold || isLastNodeMapper()){
					cache.del(key);
					
					//outputKey.set(key);
					outputDateWordPair.setDateWord(key);
					outputValue.set(valueInt);					
					context.write(outputDateWordPair, outputValue);
				}
			}
		}
		
		cache.set(prefix + taskId, "1");
		cache.incr(prefix + "NUM");
		cache.close();		
	  }
}