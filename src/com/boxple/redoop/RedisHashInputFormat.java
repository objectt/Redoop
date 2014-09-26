package com.boxple.redoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redis.clients.jedis.Jedis;

//This input format will read all the data from a given set of Redis hosts
public class RedisHashInputFormat extends InputFormat<Text, Text> {

	// Again, the CSV list of hosts and a hash key variables and methods for configuration
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
	}

	public static void setRedisHashKey(Job job, String hashKey) {
		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
	}

	// This method will return a list of InputSplit objects.
	// The framework uses this to create an equivalent number of map tasks
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		// Get our configuration values and ensure they are set
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		if (hashKey == null || hashKey.isEmpty()) {
			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
		}

		// Create an input split for each Redis instance
		// More on this custom split later, just know that one is created per host
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String host : hosts.split(",")) {
			splits.add(new RedisHashInputSplit(host, hashKey));
		}

		return splits;
	}

	// This method creates an instance of our RedisHashRecordReader
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	// This custom RecordReader will pull in all key/value pairs from a Redis instance for a given hash
	public static class RedisHashRecordReader extends RecordReader<Text, Text> {
		// A number of member variables to iterate and store key/value pairs from Redis
		private Iterator<Entry<String, String>> keyValueMapIter = null;
		private Text key = new Text(), value = new Text();
		private float processedKVs = 0, totalKVs = 0;
		private Entry<String, String> currentEntry = null;

		// Initialize is called by the framework and given an InputSplit to process
		public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

			// Get the host location from the InputSplit
			String host = split.getLocations()[0];
			String hashKey = ((RedisHashInputSplit) split).getHashKey();

			// Create a new connection to Redis
			Jedis jedis = new Jedis(host);
			jedis.connect();
			jedis.getClient().setTimeoutInfinite();

			// Get all the key/value pairs from the Redis instance and store them in memory
			totalKVs = jedis.hlen(hashKey);
			keyValueMapIter = jedis.hgetAll(hashKey).entrySet().iterator();
			//LOG.info("Got " + totalKVs + " from " + hashKey); 
			jedis.disconnect();
		}

		// This method is called by Mapper¡¯s run method to ensure all key/value pairs are read
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (keyValueMapIter.hasNext()) {
				// Get the current entry and set the Text objects to the entry
				currentEntry = keyValueMapIter.next();
				key.set(currentEntry.getKey());
				value.set(currentEntry.getValue());
				return true;
			} else {
				return false;
			}
		}

		// The next two methods are to return the current key/value pairs.  Best practice is to re-use objects rather than create new ones, i.e. don¡¯t use ¡°new¡±
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		// This method is used to report the progress metric back to the framework.  It is not required to have a true implementation, but it is recommended.
		public float getProgress() throws IOException, InterruptedException {
			return processedKVs / totalKVs;
		}

		public void close() throws IOException {
			/* nothing to do */
		}

	} // end RedisHashRecordReader

	public static class RedisHashInputSplit extends InputSplit implements Writable {

		// Two member variables, the hostname and the hash key (table name)
		private String location = null;
		private String hashKey = null;

		public RedisHashInputSplit() {
		// Default constructor required for reflection
		}

		public RedisHashInputSplit(String redisHost, String hash) {
			this.location = redisHost;
			this.hashKey = hash;
		}

		public String getHashKey() {
			return this.hashKey;
		}

		// The following two methods are used to serialize the input information for an individual task
		public void readFields(DataInput in) throws IOException {
			this.location = in.readUTF();
			this.hashKey = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(location);
			out.writeUTF(hashKey);
		}

		// This gets the size of the split so the framework can sort them by size.  This isn¡¯t that important here, but we could query a Redis instance and get the bytes if we desired
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// This method returns hints to the framework of where to launch a task for data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { location };
		}

	} // end RedisHashInputSplit

} // end RedisHashInputFormat
