package com.boxple.redoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;	


public class Redoop extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Redoop(), args);
        System.exit(res);       
    }
    
    @Override
    public int run(String[] args) throws Exception {
    	//if (args.length != 2) {
    	//	System.out.println("usage: [input] [output]");
    	//	System.exit(-1);
    	//}
        
        //Configuration conf = new Configuration(true);
        Configuration conf = this.getConf();
        //args = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        conf.set("mapreduce.redis.host", "127.0.0.1");
        conf.set("mapreduce.redis.port", "7000");
        //conf.set("mapred.redishashinputformat.key", "test");
  
        Job job = Job.getInstance(conf);
        job.setJarByClass(Redoop.class);
        job.setJobName(getClass().getSimpleName());
        
        // Redoop
        job.setMapperClass(RedisMapper.class);
        job.setReducerClass(RedisReducer.class);
        //job.setPartitionerClass(RedisPartitioner.class);
        //job.setCombinerClass(RedisCombiner.class);
        job.setInputFormatClass(RedisHashInputFormat.class);
        //job.setOutputFormatClass(RedisHashOutputFormat.class);   
        FileOutputFormat.setOutputPath(job, new Path("/grep/output-redoop"));      
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Normal HD
//        job.setMapperClass(HadoopMapper.class);
//        job.setReducerClass(HadoopReducer.class);
//        FileInputFormat.setInputPaths(job, new Path("/grep/in"));
//        FileOutputFormat.setOutputPath(job, new Path("/grep/output"));
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        //job.setM(2);
        //job.setNumReduceTasks(4);

        return job.waitForCompletion(true) ? 0 : 1;
	}
}