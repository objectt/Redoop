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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;
//import redis.clients.jedis.exceptions.JedisMovedDataException;


public class RedisHashInputFormat extends InputFormat<Text, Text> {
	private static final Log LOG = LogFactory.getLog(RedisHashInputFormat.class);
	
	public static final String REDIS_HOST_CONF = "mapreduce.redis.host"; // Master Redis
	public static final String REDIS_PORT_CONF = "mapreduce.redis.port";
	
	public static JedisCluster jedis;

	public static void setRedisMaster(Job job, String host) {
		job.getConfiguration().set(REDIS_HOST_CONF, host);
	}

	// Ran by AppMaster once
	// Create an equivalent number of map tasks = # of Redis nodes
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		LOG.info("RedisHashInputFormat::getSplits");
		
		String host = job.getConfiguration().get(REDIS_HOST_CONF);
		if (host == null || host.isEmpty()) {
			throw new IOException(REDIS_HOST_CONF + " is not set in configuration.");
		}
		
		String port = job.getConfiguration().get(REDIS_PORT_CONF);
		if (port == null || port.isEmpty()) {
			throw new IOException(REDIS_PORT_CONF + " is not set in configuration.");
		}

		// Create an input split for each Redis instance (Each host)
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort(host, Integer.parseInt(port)));
		jedis = new JedisCluster(jedisClusterNodes);
		
		// Get cluster info
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String hosts : jedis.getClusterNodes().keySet()){
			String[] uri = hosts.split(":");			
			splits.add(new RedisHashInputSplit(uri[0], uri[1]));	// Host, Port
		}
		jedis.close();
		
		return splits;
	}

	// This method creates an instance of our RedisHashRecordReader
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	// Ran by each container
	public static class RedisHashRecordReader extends RecordReader<Text, Text> {
		
		private Iterator<Entry<String, String>> keyValueMapIter = null;
		//private Map keyValueMap = new HashMap();
		//private Set keyValueSet = new HashSet();
		
		private Text key = new Text(), value = new Text();
		private int processedKVs = 0, totalKVs = 0;
		private Entry<String, String> currentEntry = null;
		private Jedis jedisInstance;
		private List<String> keys;

		// Ran by each Mapper
		// Initialize is called by the framework and given an InputSplit to process
		@SuppressWarnings("deprecation")
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
			
			RedisHashInputSplit split = (RedisHashInputSplit) genericSplit;
			
			LOG.info("RedisHashInputFormat::initialize");
			LOG.info("Split.addr = " + split.getHost() + ":" + split.getPort());
			
			jedisInstance = new Jedis(split.getHost(), split.getPort());
			jedisInstance.connect();
			//jedisInstance.getClient().setTimeoutInfinite();
			
			ScanResult<String> scanResult = jedisInstance.scan(0);
            List<String> result = scanResult.getResult();
            int cursor = scanResult.getCursor();

            List<String> keys = new ArrayList<String>(result);

            while(cursor > 0){                    
                    scanResult = jedisInstance.scan(cursor);
                    result = scanResult.getResult();
                    keys.addAll(result);
                    cursor = scanResult.getCursor();                   
            }
            
            totalKVs = keys.size();
			
			// Get the host location from the InputSplit
			//String host = split.getLocations()[0];
			//String hashKey = ((RedisHashInputSplit) split).getHashKey();			
			//Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
			//jedisClusterNodes.add(new HostAndPort("127.0.0.1", 7000));
			//jedisClusterNodes.add(new HostAndPort(split.getHost(), split.getPort()));
			//jedis = new JedisCluster(jedisClusterNodes);	
			//keyValueMapIter = jedis.hgetAll("member:" + currentKey).entrySet().iterator();			
		}

		// Called by Mapper
		public boolean nextKeyValue() throws IOException, InterruptedException {			
			String currentHashKey;
			
			if(!keys.isEmpty()){
				currentHashKey = keys.get(0);
				keyValueMapIter = jedisInstance.hgetAll(currentHashKey).entrySet().iterator();
				
				key.set(currentHashKey);
				value.set("");
				
				while(keyValueMapIter.hasNext()){
					currentEntry = keyValueMapIter.next();
					value.set(value.toString() + ',' + currentEntry.getValue());
				}
				
				processedKVs++;
				keys.remove(currentHashKey);
				
				return true;
			}
//			
//			if(currentKey < totalKVs){
//				String currentHashKey = "member:" + Integer.toString(currentKey);
//				
//				try{
//					exists = jedisInstance.exists(currentHashKey);
//					System.out.println("nextKeyValue() exists = true");
//				}catch(JedisMovedDataException e){
//					exists = false;
//					System.out.println("nextKeyValue() exists = false");
//					return true;
//				}
//				
//				if(exists){
//					keyValueMapIter = jedisInstance.hgetAll(currentHashKey).entrySet().iterator();
//					key.set(currentHashKey);
//					value.set("");
//					while(keyValueMapIter.hasNext()){
//						currentEntry = keyValueMapIter.next();
//						value.set(value.toString() + ',' + currentEntry.getValue());
//					}
//					processedKVs++;
//					//return true;
//				}
//				
//				currentKey++;				
//				return true;
//			}
			
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
			jedisInstance.close();
		}

	} // end RedisHashRecordReader

	public static class RedisHashInputSplit extends InputSplit implements Writable {

		private String host = null;
		private String port = null;

		public RedisHashInputSplit() {
			// Default constructor required for reflection
		}

		public RedisHashInputSplit(String redisHost, String redisPort) {
			this.host = redisHost;
			this.port = redisPort;
		}

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

		// This gets the size of the split so the framework can sort them by size.
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// This method returns hints to the framework of where to launch a task for data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { host };
		}

	} // end RedisHashInputSplit

} // end RedisHashInputFormat
