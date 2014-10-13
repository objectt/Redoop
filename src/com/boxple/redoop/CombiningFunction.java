package com.boxple.redoop;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public interface CombiningFunction<VALUE extends Writable> {
   // public VALUE combine(VALUE value1, VALUE value2);

	public VALUE combine(VALUE value1, VALUE value2);
}