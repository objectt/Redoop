package com.boxple.redoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;


public class RedisHashInputFormat extends InputFormat<Text, Text> {

	// Should connect to Redis Manager for host info
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";

	public static JedisCluster jedis;
	
	private static final Log LOG = LogFactory.getLog(RedisHashInputFormat.class);
	
	public static void setRedisMaster(Job job, String host){
		
	}

	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
	}

	public static void setRedisHashKey(Job job, String hashKey) {
		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
	}

	// This method will return a list of InputSplit objects.
	// The framework uses this to create an equivalent number of map tasks
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		//System.out.println("RedisHashInputFormat::getSplits");
		LOG.info("RedisHashInputFormat::getSplits");
		// Get our configuration values and ensure they are set
//		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
//		if (hosts == null || hosts.isEmpty()) {
//			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
//		}
//
//		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
//		if (hashKey == null || hashKey.isEmpty()) {
//			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
//		}

		// Create an input split for each Redis instance
		// More on this custom split later, just know that one is created per host
		//for (JedisPool pool : jedis.getClusterNodes().values())){
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7000));
		jedis = new JedisCluster(jedisClusterNodes);
		
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String host : jedis.getClusterNodes().keySet()){
			//System.out.println(key);
			String[] uri = host.split(":");
			
			splits.add(new RedisHashInputSplit(uri[0], uri[1]));
		}
		jedis.close();
//		List<InputSplit> splits = new ArrayList<InputSplit>();
//		for (String host : hosts.split(",")) {
//			splits.add(new RedisHashInputSplit(host, hashKey));
//		}

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
		//private Map keyValueMap = new HashMap();
		//private Set keyValueSet = new HashSet();
		
		private Text key = new Text(), value = new Text();
		private int processedKVs = 0, totalKVs = 0, currentKey = 1;
		private Entry<String, String> currentEntry = null;
		//private Jedis jedis = new Jedis("localhost");

		// Ran by each Mapper
		// Initialize is called by the framework and given an InputSplit to process
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

			LOG.info("RedisHashInputFormat::initialize");
			
			RedisHashInputSplit split = (RedisHashInputSplit) genericSplit;
			
			// Get the host location from the InputSplit
			//String host = split.getLocations()[0];
			//String hashKey = ((RedisHashInputSplit) split).getHashKey();

			// Create a new connection to Redis
			//Jedis jedis = new Jedis(host);
			//jedis.connect();
			//jedis.getClient().setTimeoutInfinite();
			
			Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
			//jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7000));
			jedisClusterNodes.add(new HostAndPort(split.getHost(), split.getPort()));
			jedis = new JedisCluster(jedisClusterNodes);

			//int householdCounter = 1000;
			
			//totalKVs = jedis.hlen(hashKey);
			//totalKVs = jedis.keys('*');
			//totalKVs = Integer.parseInt(jedis.get("member_counter"));
			totalKVs = 100000;
			//keyValueMapIter = jedis.hgetAll("member:" + currentKey).entrySet().iterator();
			
			//jedis.disconnect();
			//jedis.close();
		}

		// This method is called by Mapper¡¯s run method to ensure all key/value pairs are read
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
			if(currentKey < totalKVs){
				String currentHashKey = "member:" + Integer.toString(currentKey);
				//System.out.println("CurrentKey = " + currentHashKey);
				
				keyValueMapIter = jedis.hgetAll(currentHashKey).entrySet().iterator();
				key.set(currentHashKey);
				value.set("");
				while(keyValueMapIter.hasNext()){
					currentEntry = keyValueMapIter.next();
					value.set(value.toString() + ',' + currentEntry.getValue());
				}
				
				//System.out.println("CurrentValue = " + value.toString());
				
				currentKey++;
				return true;
			}
			
			return false;
			
			
//			if (keyValueMapIter.hasNext()) {
//				// Get the current entry and set the Text objects to the entry
//				currentEntry = keyValueMapIter.next();
//				key.set(currentEntry.getKey());	
//				value.set(currentEntry.getValue());
//				return true;
//			} else {
//				return false;
//			}
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
			//jedis.disconnect();
			jedis.close();
		}

	} // end RedisHashRecordReader

	public static class RedisHashInputSplit extends InputSplit implements Writable {

		// Two member variables, the hostname and the hash key (table name)
		private String host = null;
		private String port = null;
		//private String hashKey = null;

		public RedisHashInputSplit() {
		// Default constructor required for reflection
		}

		public RedisHashInputSplit(String redisHost, String redisPort) {
			this.host = redisHost;
			this.port = redisPort;
			//this.hashKey = hash;
		}

//		public String getHashKey() {
//			return this.hashKey;
//		}
		
		public String getHost(){
			return this.host;
		}
		
		public int getPort(){
			return Integer.parseInt(this.port);
		}

		// The following two methods are used to serialize the input information for an individual task
		public void readFields(DataInput in) throws IOException {
			this.host = in.readUTF();
			this.port = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(host);
			out.writeUTF(port);
		}

		// This gets the size of the split so the framework can sort them by size.  This isn¡¯t that important here, but we could query a Redis instance and get the bytes if we desired
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// This method returns hints to the framework of where to launch a task for data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { host };
		}

	} // end RedisHashInputSplit

} // end RedisHashInputFormat
