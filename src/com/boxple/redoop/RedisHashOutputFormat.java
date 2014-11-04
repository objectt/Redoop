package com.boxple.redoop;

import java.io.IOException;
//import java.util.HashMap;
import java.util.HashSet;
//import java.util.Map;
import java.util.Set;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisHashOutputFormat extends OutputFormat<Text, Text> {

	//private static final Log LOG = LogFactory.getLog(RedisHashOutputFormat.class);
	public static final String REDIS_HOST_CONF = "mapreduce.redis.host";
	public static final String REDIS_PORT_CONF = "mapreduce.redis.port";
	protected Configuration redisConf;
		
	// Ran once
	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
		throws IOException, InterruptedException {
		
		redisConf = job.getConfiguration();
		return new RedisHashRecordWriter(redisConf.get(REDIS_HOST_CONF), Integer.parseInt(redisConf.get(REDIS_PORT_CONF)));
	}

	@Override
	public void checkOutputSpecs(JobContext job) throws IOException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
		throws IOException, InterruptedException {
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static class RedisHashRecordWriter extends RecordWriter<Text, Text> {
		public static JedisCluster jedisCluster;
		String mrOutputKey = "MR:RESULT:";
		
		public RedisHashRecordWriter(String host, int port) {			
			Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
			jedisClusterNodes.add(new HostAndPort(host, port));
			jedisCluster = new JedisCluster(jedisClusterNodes);
		}

		public void write(Text key, Text value) throws IOException, InterruptedException {
			//Map<String, String> hvalue = new HashMap<String, String>();			
			//hvalue.put(key.toString(), value.toString());
			
			System.out.println("REDUCER::(" + key.toString() + ", " + value.toString() + ")");
			
			jedisCluster.set(mrOutputKey + key.toString(), value.toString());
			//jedisCluster.hset(mrOutputKey, key.toString(), value.toString());
			//jedisCluster.set(mrOutputKey + key.toString(), value.toString());			
			//jedisCluster.hmset(mrOutputKey, hvalue);
			//jedisCluster.set(key.toString(), value.toString());
		}

		public void close(TaskAttemptContext context)
			throws IOException, InterruptedException {
			
			jedisCluster.close();
		}
	}
} // end RedisHashOutputFormat
