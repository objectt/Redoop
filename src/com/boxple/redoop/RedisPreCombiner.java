package com.boxple.redoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
//import redis.clients.jedis.exceptions.JedisMovedDataException;

import java.io.IOException;
//import java.util.LinkedHashMap;
//import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public class RedisPreCombiner<KEY extends Writable, VALUE extends Writable> {
	  private static final int DEFAULT_CAPACITY = 8388608;
	  private static final int DEFAULT_INITIAL_CAPACITY = 65536;
	  private static final float DEFAULT_LOAD_FACTOR = .75F;
	  //private int maxCacheCapacity;
	  //private Map < KEY, VALUE > lruCache;
	  
	  private CombiningFunction < VALUE > combiningFunction;
	  
	  private String keyPrefix = "MR:IR:";
	  private VALUE prevValue = null;
	  private String tempValue = null;
	  
	  private Jedis jedisInstance;
	  private JedisCluster jedisCluster;
	  
	  private Text outputKey = new Text();
	  private IntWritable outputValue = new IntWritable();
	  
	  public RedisPreCombiner(int cacheCapacity, CombiningFunction<VALUE> combiningFunction, int initialCapacity, float loadFactor) {
	    this.combiningFunction = combiningFunction;
	    //this.maxCacheCapacity = cacheCapacity;	    


		// Not USED!
//	    lruCache = new LinkedHashMap<KEY, VALUE>(initialCapacity, loadFactor, true) {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//            @SuppressWarnings("unchecked")
//	    	protected boolean removeEldestEntry(Map.Entry < KEY, VALUE > eldest) {
//		        boolean isFull = size() > maxCacheCapacity;
//		        if (isFull) {
//		          try {
//		            // If the cache is full, emit the eldest key value pair to the reducer, and delete them from cache
//		        	System.out.println("PreCombiner::removeEldestEntry - context.write");
//		            context.write(eldest.getKey(), eldest.getValue());
//		          } catch (IOException ex) {
//		            throw new UncheckedIOException(ex);
//		          } catch (InterruptedException ex) {
//		            throw new UncheckedInterruptedException(ex);
//		          }
//		        }
//		        return isFull;
//	      }
//	    };
	  }
	  
	  // Initial Constructor
	  public RedisPreCombiner() {
	    this(DEFAULT_CAPACITY, null, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	  }
	  
	  public RedisPreCombiner(int cacheCapacity, CombiningFunction < VALUE > combiningFunction) {
	    this(cacheCapacity, combiningFunction, 512, .75F);
	  }
	  
	  public RedisPreCombiner(int cacheCapacity) {
	    this(cacheCapacity, null, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	  }

	  public RedisPreCombiner(CombiningFunction < VALUE > combiningFunction) {
	    this(DEFAULT_CAPACITY, combiningFunction, 512, .75F);
	  }
 
	  public void setPort(int splitPort){
		//this.port = splitPort;
		  
	    // Redis Instance
		jedisInstance = new Jedis("127.0.0.1", splitPort);
		jedisInstance.getClient().setTimeoutInfinite();
		jedisInstance.connect();
		
		// Redis Cluster
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", splitPort));
		jedisCluster = new JedisCluster(jedisClusterNodes);
	  }
	  
//	  public void setCombiningFunction(CombiningFunction < VALUE > combiningFunction) {
//	    this.combiningFunction = combiningFunction;
//	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void write(KEY key, VALUE value, Mapper.Context context) throws InterruptedException, IOException {
			//this.context = context;			
		    Configuration conf = context.getConfiguration();
			
			key = WritableUtils.clone(key, conf);
			value = WritableUtils.clone(value, conf);
			
			System.out.println("COMBINER1::(" + key.toString() + ", " + value.toString() + ")");
			
			// Store intermediate result in local Redis
		    if (combiningFunction != null) {
		    	try {
		    		tempValue = jedisCluster.get(keyPrefix + key.toString());
		    		prevValue = (VALUE) new IntWritable(Integer.parseInt(tempValue));
		    		
		    		System.out.println("COMBINER2::(" + keyPrefix + key.toString() + ", " + tempValue + ")");		    		
		    		
		    		jedisCluster.set(keyPrefix + key.toString(), combiningFunction.combine(prevValue, value).toString());
		    		
			        //if (!lruCache.containsKey(key)) {
			        //  lruCache.put(key, value);
			        //} else {
			        //  lruCache.put(key, combiningFunction.combine(lruCache.get(key), value));
			        //}
		    	} catch(Exception ex){
		    		jedisCluster.set(keyPrefix + key.toString(), value.toString());
		    		//jedisInstance.set(keyPrefix + key.toString(), value.toString());
		    	}
	    	} else {
	    		context.write(key, value);
			}
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void flush(Mapper.Context context) throws IOException, InterruptedException {
	    //if (!lruCache.isEmpty()) {	
	    //for (Map.Entry < KEY, VALUE > item: lruCache.entrySet()) {
	    //}
		//lruCache.clear();
		  
	  	for(String key : jedisInstance.keys(keyPrefix + "*")){
	  		outputKey.set(key);
	  		outputValue.set(Integer.parseInt(jedisInstance.get(key)));
	  		jedisInstance.del(key);
    		
    		context.write(outputKey, outputValue);
    		System.out.println("COMBINER::flush - context.write(" + key + "," + outputValue.toString() + ")");
    		//context.write(item.getKey(), item.getValue());
    	}

	    jedisInstance.close();
	   
	  }
	  
	  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//	  private static class UncheckedIOException extends java.lang.RuntimeException {
//		private static final long serialVersionUID = 1L;
//
//		@SuppressWarnings("unused")
//		public UncheckedIOException(Throwable throwable) {
//	      super(throwable);
//	    }
//	  }
//
//	  private static class UncheckedInterruptedException extends java.lang.RuntimeException {
//		private static final long serialVersionUID = 1L;
//
//		@SuppressWarnings("unused")
//		public UncheckedInterruptedException(Throwable throwable) {
//	      super(throwable);
//	    }
//	  }
	}