package com.boxple.redoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RedisPreCombiner<KEY extends Writable, VALUE extends Writable> {
	  private static final int DEFAULT_CAPACITY = 8388608;
	  private static final int DEFAULT_INITIAL_CAPACITY = 65536;
	  private static final float DEFAULT_LOAD_FACTOR = .75F;

	  private int maxCacheCapacity;
	  private Map < KEY, VALUE > lruCache;
	  private CombiningFunction < VALUE > combiningFunction;
	 
	  @SuppressWarnings("rawtypes")
	  private Mapper.Context context;
	  
	  public RedisPreCombiner(int cacheCapacity, CombiningFunction<VALUE> combiningFunction, int initialCapacity, float loadFactor) {
	    this.combiningFunction = combiningFunction;
	    this.maxCacheCapacity = cacheCapacity;

	    lruCache = new LinkedHashMap<KEY, VALUE>(initialCapacity, loadFactor, true) {
			private static final long serialVersionUID = 1L;

			@Override
            @SuppressWarnings("unchecked")
	    	protected boolean removeEldestEntry(Map.Entry < KEY, VALUE > eldest) {
		        boolean isFull = size() > maxCacheCapacity;
		        if (isFull) {
		          try {
		            // If the cache is full, emit the eldest key value pair to the reducer, and delete them from cache
		            context.write(eldest.getKey(), eldest.getValue());
		          } catch (IOException ex) {
		            throw new UncheckedIOException(ex);
		          } catch (InterruptedException ex) {
		            throw new UncheckedInterruptedException(ex);
		          }
		        }
		        return isFull;
	      }
	    };
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
 
	  
	  public void setCombiningFunction(CombiningFunction < VALUE > combiningFunction) {
	    this.combiningFunction = combiningFunction;
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void write(KEY key, VALUE value, Mapper.Context context) throws InterruptedException, IOException {
		    /**
		     *  In Hadoop, context.write(key, value) should have a new copy of key and value in context.write().
		     *  This is because lots of time, people use private final static LongWritable one = new LongWritable(1);
		     *  in the mapper class as global variable, and we don't want to reference to it in the LRU cache
		     *  which may cause unexpected result. As a result, we will clone the key and value objects.
		     * */
		  
			Configuration conf = context.getConfiguration();
			key = WritableUtils.clone(key, conf);
			value = WritableUtils.clone(value, conf);
			this.context = context;
		    
		    if (combiningFunction != null) {
		    	try {
			        if (!lruCache.containsKey(key)) {
			          lruCache.put(key, value);
			        } else {
			          lruCache.put(key, combiningFunction.combine(lruCache.get(key), value));
			        }
		        } catch (UncheckedIOException ex) {
		        	throw new IOException(ex);
	        	} catch (UncheckedInterruptedException ex) {
	        		throw new InterruptedException(ex.toString());
	    		}
	    	} else {
	    		context.write(key, value);
			}
	  }

	  @SuppressWarnings({ "rawtypes", "unchecked" })
	  public void flush(Mapper.Context context) throws IOException, InterruptedException {
	    // Emit the key-value pair from the LRU cache.
	    if (!lruCache.isEmpty()) {
	      for (Map.Entry < KEY, VALUE > item: lruCache.entrySet()) {
	        context.write(item.getKey(), item.getValue());
	      }
	    }
	    lruCache.clear();
	  }
	  
	  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	  private static class UncheckedIOException extends java.lang.RuntimeException {
		private static final long serialVersionUID = 1L;

		public UncheckedIOException(Throwable throwable) {
	      super(throwable);
	    }
	  }

	  private static class UncheckedInterruptedException extends java.lang.RuntimeException {
		private static final long serialVersionUID = 1L;

		public UncheckedInterruptedException(Throwable throwable) {
	      super(throwable);
	    }
	  }
	}