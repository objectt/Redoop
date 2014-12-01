package com.boxple.redoop;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.text.ParseException;

public class CombinerNode<KEY extends Writable, VALUE extends Writable> {
	
	  private Jedis cache;
	  private Pipeline pCache;
	  private CombiningFunction < VALUE > combiningFunction;
	  private int minThreshold, mapperCnt;
	  
	  private int taskId;
	  private String prefix = "MR:JOB:";
	  private String keyStr, valueStr;
	  
	  // Should fix these to match data types
	  private int valueInt;
	  private IntWritable outputValue = new IntWritable();
	  private DateWordPair outputDateWordPair = new DateWordPair();
	  
	  boolean isLastNodeMapper = false;
	  int recordCnt = 0;
	  
	  public CombinerNode(CombiningFunction<VALUE> combiningFunction, int port){
		  this.combiningFunction = combiningFunction;
		  
		  cache = new Jedis("127.0.0.1", port);
		  cache.getClient().setTimeoutInfinite();
		  cache.connect();
		  
		  pCache = cache.pipelined();								// Redis pipeline
		  
		  cache.setnx("MR:JOB:TOTAL", "0");							// Initialize
		  mapperCnt = Integer.parseInt(cache.get("MR:JOB:TOTAL"));	// Total number of tasks allocated to this node  redis-cli -p 7003 set MR:JOB:TOTAL 3
	  }
	  
	  public void initCombiner(int tid, String threshold){
		  minThreshold = Integer.parseInt(threshold);
		  taskId = tid;
		  pCache.set(prefix + taskId, "0");
		  pCache.setnx("MR:JOB:DONE", "0");		  
	  }
	  
//	  public boolean isLastNodeMapper(){		  
//		  return Integer.parseInt(cache.get("MR:JOB:DONE")) == (mapperCnt - 1);		// redis-cli -p 7003 set MR:JOB:DONE 0
//	  }
	  
	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void write(KEY key, VALUE value, Mapper.Context context) 
			  throws InterruptedException, IOException {

		  recordCnt++;
		  key = WritableUtils.clone(key, context.getConfiguration());
		  value = WritableUtils.clone(value, context.getConfiguration());
		  		  
		  keyStr = key.toString();
		  //valueStr = value.toString();
		  valueInt = Integer.parseInt(value.toString());	// Fix

		  if (combiningFunction != null){
			try {
				//cache.incrBy(keyStr, valueInt);
				pCache.incrBy(keyStr, valueInt);
				
				if(recordCnt % 50 == 0){
					pCache.sync();
					pCache = cache.pipelined();
				}
			} catch(Exception ex){}
		  } else {
			context.write(key, value);
		  }
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void flush(Mapper.Context context) throws IOException, InterruptedException, ParseException {
		  pCache.sync();
		  isLastNodeMapper = Integer.parseInt(cache.get("MR:JOB:DONE")) == (mapperCnt - 1);
		  
		  for(String key : cache.keys("*")){
			  if(!(key.substring(0, prefix.length())).equals(prefix)){
				  
				  //valueStr = cache.get(key);
				  if((valueStr = cache.get(key)) == null) continue;
				  
				  valueInt = Integer.parseInt(valueStr);
				
				  if(valueInt > minThreshold || isLastNodeMapper){
					cache.del(key);
					
					//outputKey.set(key);
					outputDateWordPair.setDateWord(key);
					outputValue.set(valueInt);
					context.write(outputDateWordPair, outputValue);
				}
			}
		}
		
		cache.set(prefix + taskId, "1");
		cache.incr(prefix + "DONE");
		cache.close();		
	  }
}