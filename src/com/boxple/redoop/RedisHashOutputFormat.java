package com.boxple.redoop;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import redis.clients.jedis.Jedis;

// This output format class is templated to accept a key and value of type Text
public class RedisHashOutputFormat extends OutputFormat<Text, Text> {

	// These static conf variables and methods are used to modify the job configuration.  
	// This is a common pattern for MapReduce related classes to avoid the magic string problem
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
	}

	public static void setRedisHashKey(Job job, String hashKey) {
		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
	}

	// Returns an instance of a RecordWriter for the task.  Note how we are pulling the variables set by the static methods during configuration
	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job)
		throws IOException, InterruptedException {
		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		return new RedisHashRecordWriter(hashKey, csvHosts);
	}

	// This method is used on the front-end prior to job submission to ensure everything is configured correctly
	@Override
	public void checkOutputSpecs(JobContext job) throws IOException {
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		if (hashKey == null || hashKey.isEmpty()) {
			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
		}
	}

	// The output committer is used on the back-end to, well, commit output.  Discussion of this class is out of scope, but more info can be found here
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
		throws IOException, InterruptedException {
		// use a null output committer, since
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	public static class RedisHashRecordWriter extends RecordWriter<Text, Text> {
		// This map is used to map an integer to a Jedis instance
		private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();

		// This is the name of the Redis hash
		private String hashKey = null;

		public RedisHashRecordWriter(String hashKey, String hosts) {
			this.hashKey = hashKey;

			// Create a connection to Redis for each host
			// Map an integer 0-(numRedisInstances - 1) to the instance
			int i=0;
			for (String host : hosts.split(",")) {
				Jedis jedis = new Jedis(host);
				jedis.connect();
				jedisMap.put(i++, jedis);
			}
		}

		// The write method is what will actually write the key value pairs out to Redis
		public void write(Text key, Text value) throws IOException, InterruptedException {
			// Get the Jedis instance that this key/value pair will be written to.
			Jedis j = jedisMap.get(Math.abs(key.hashCode()) % jedisMap.size());

			// Write the key/value pair
			j.hset(hashKey, key.toString(), value.toString());
		}

		public void close(TaskAttemptContext context)
			throws IOException, InterruptedException {
			
			// For each jedis instance, disconnect it
			for (Jedis jedis : jedisMap.values()) {
				jedis.disconnect();
			}
		}
	}
	
} // end RedisHashOutputFormat
