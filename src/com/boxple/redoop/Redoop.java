package com.boxple.redoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;	


public class Redoop extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Redoop(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
    	FileSystem hdfs = FileSystem.get(conf);
    	hdfs.delete(new Path("/grep/output-redoop"), true);
    			
        conf.set("mapreduce.redis.host", "147.46.121.158");
        conf.set("mapreduce.redis.port", "7000");
  
        Job job = Job.getInstance(conf);
        job.setJobName("Redoop Word Counter");
        job.setJarByClass(Redoop.class);
        
        job.setMapperClass(RedisMapper.class);
        job.setReducerClass(RedisReducer.class);
        
        //job.setPartitionerClass(RedisPartitioner.class);
        //job.setCombinerClass(RedisCombiner.class);
        
        job.setInputFormatClass(RedisHashInputFormat.class);
        job.setOutputFormatClass(RedisHashOutputFormat.class);    
        //FileOutputFormat.setOutputPath(job, new Path("/grep/output-redoop"));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);      
   
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
//        job.setMapperClass(HadoopMapper.class);
//        job.setReducerClass(HadoopReducer.class);
//        FileInputFormat.setInputPaths(job, new Path("/grep/in"));
//        FileOutputFormat.setOutputPath(job, new Path("/grep/output"));
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);             
        job.setNumReduceTasks(1);
        
        return job.waitForCompletion(true) ? 0 : 1;
	}
}