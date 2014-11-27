package com.boxple.redoop;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class CombinerPre<KEY extends Writable, VALUE extends Writable> {
	
	  private static final int DEFAULT_CAPACITY = 10000000;
	  private static final int MAX_CAPACITY = 70000000;
	  //private static final int DEFAULT_CAPACITY = 75536;
	  //private static final int DEFAULT_INITIAL_CAPACITY = 65536;
	  private static final float DEFAULT_LOAD_FACTOR = .75F;	 
	  	
	  private Map < KEY, VALUE > lruCache;
	  private int maxCacheCapacity;
	  private CombiningFunction < VALUE > combiningFunction;	 
	  
	  @SuppressWarnings("rawtypes")
	  private Context mContext;
	  
	  @SuppressWarnings("rawtypes")
	  public void setContext(Mapper.Context context){
		  this.mContext = context;
	  }

	  // Constructor
	  public CombinerPre(CombiningFunction<VALUE> combiningFunction){

	    this.maxCacheCapacity = MAX_CAPACITY;
	    this.combiningFunction = combiningFunction;

	    lruCache = new LinkedHashMap<KEY, VALUE>(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, true) {
			private static final long serialVersionUID = 1L;

			@Override
            @SuppressWarnings("unchecked")
	    	protected boolean removeEldestEntry(Map.Entry < KEY, VALUE > eldest) {
		        boolean isFull = size() > maxCacheCapacity;
		        if (isFull) {
		          try {
		        	  //System.out.println("LRU - " + eldest.getKey().toString() + "," + eldest.getValue().toString());
		        	  mContext.write(eldest.getKey(), eldest.getValue());
		          } catch (Exception ex) {
		            //throw new UncheckedIOException(ex);
		          } 
		        }
		        return isFull;
	      }
	    };
	  }
	  

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void write(KEY key, VALUE value, Mapper.Context context) 
			  throws InterruptedException, IOException {

		key = WritableUtils.clone(key, context.getConfiguration());
		value = WritableUtils.clone(value, context.getConfiguration());
		
		if (combiningFunction != null) {
			try {				
		        if (!lruCache.containsKey(key)) {
		        	lruCache.put(key, value);
		        	//System.out.println("NO CACHE - " + key.toString() + "," + value.toString());
		        } else {
		        	lruCache.put(key, combiningFunction.combine(lruCache.get(key), value));
		        	//System.out.println("YES CACHE - " + key.toString() + "," + value.toString());
		        }
			} catch(Exception ex){}
		} else {
			context.write(key, value);
		}
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void flush(Mapper.Context context) throws IOException, InterruptedException {
	    if (!lruCache.isEmpty()) {	
	    	for (Map.Entry < KEY, VALUE > item: lruCache.entrySet()) {
	    		context.write(item.getKey(), item.getValue());
	    	}
	    }
		lruCache.clear();	   
	  }
}