package com.boxple.redoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
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
        
        Configuration conf = new Configuration(true);
        conf.set("mapred.redishashinputformat.hosts", "localhost");
        conf.set("mapred.redishashinputformat.key", "household");
  
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(RedisMapper.class); 
        job.setReducerClass(RedisReducer.class);  

        job.setInputFormatClass(RedisHashInputFormat.class);
        job.setOutputFormatClass(RedisHashOutputFormat.class);
        
        //RedisHashInputFormat.setInputPaths(job, new Path(args[0]));
        //RedisHashOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(Redoop.class);

        job.submit();
		return 0;    
	}
}