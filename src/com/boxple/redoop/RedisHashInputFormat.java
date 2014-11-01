package com.boxple.redoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class RedisHashInputFormat extends InputFormat<Text, Text> {
	
	private static final Log LOG = LogFactory.getLog(RedisHashInputFormat.class);

	/*
	 * Class : RedisHashInputFormat.RedisHashInputSplit
	 * 
	 */
	public static class RedisHashInputSplit extends InputSplit implements Writable {

		private String host = null;
		private String port = null;

		public RedisHashInputSplit() {}

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

		public void readFields(DataInput in) throws IOException {
			this.host = in.readUTF();
			this.port = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(host);
			out.writeUTF(port);
		}

		// All splits have equal size?
		// * get DBSize?
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// Data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { host };
		}

	} // end RedisHashInputSplit
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	public static final String REDIS_HOST_CONF = "mapreduce.redis.host";
	public static final String REDIS_PORT_CONF = "mapreduce.redis.port";
	public static final String REDIS_KEY_NAMES = "mapreduce.redis.keys";
	
	public static JedisCluster jedisCluster;	
	protected String[] keyNames;
	protected Configuration redisConf;
	
	
	public void setConf(Configuration conf) {		
		redisConf = conf;
		
		try{
			getClusterConnection();
		} catch(Exception ex){
			throw new RuntimeException(ex);
		} 
		
		//keyNames = conf.get(REDIS_KEY_NAMES).split(",");
	}
	
	public void getClusterConnection(){
		
//		String host = job.getConfiguration().get(REDIS_HOST_CONF);
//		if (host == null || host.isEmpty()) {
//			throw new IOException(REDIS_HOST_CONF + " is not set in configuration.");
//		}
//		
//		String port = job.getConfiguration().get(REDIS_PORT_CONF);
//		if (port == null || port.isEmpty()) {
//			throw new IOException(REDIS_PORT_CONF + " is not set in configuration.");
//		}
		
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort(redisConf.get(REDIS_HOST_CONF), Integer.parseInt(redisConf.get(REDIS_PORT_CONF))));
		jedisCluster = new JedisCluster(jedisClusterNodes);
				
		//return jedisCluster;
	}

	// Ran by AppMaster once
	// Create an equivalent number of map tasks = # of Redis nodes
	// * Can be improved by splitting based on dbSize (average record size)
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		LOG.info("RedisHashInputFormat::getSplits");		
		setConf(job.getConfiguration());
				
		try{
			List<InputSplit> splits = new ArrayList<InputSplit>();
			for (String hosts : jedisCluster.getClusterNodes().keySet()){
				String[] uri = hosts.split(":");			
				splits.add(new RedisHashInputSplit(uri[0], uri[1]));	// Host, Port
			}		
			
			return splits;
		} catch(Exception ex){
			throw new RuntimeException(ex.getMessage());
		} finally{
			jedisCluster.close();
		}
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	// Ran by each container
	public static class RedisHashRecordReader extends RecordReader<Text, Text> {
		
		private Iterator<Entry<String, String>> keyValueMapIter = null;		
		private Text key = new Text(), value = new Text();
		private int processedKVs = 0, totalKVs = 0;
		private Entry<String, String> currentEntry = null;
		private Jedis jedisInstance;
		private List<String> keys = new ArrayList<String>();

		// Ran by each Mapper once
		// Initialize is called by the framework and given an InputSplit to process
		// Data locality
		@SuppressWarnings("deprecation")
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
			
			RedisHashInputSplit split = (RedisHashInputSplit) genericSplit;
			
			LOG.info("RedisHashRecordReader::initialize");
			LOG.info("Split.addr = " + split.getHost() + ":" + split.getPort());
			LOG.info("My IP = " + InetAddress.getLocalHost().getHostAddress());
			
			jedisInstance = new Jedis(split.getHost(), split.getPort());
			jedisInstance.getClient().setTimeoutInfinite();
			jedisInstance.connect();
			
			ScanParams params = new ScanParams();
			params.count(1000);
			
			// Get a list of keys
			// **** Possible bottleneck spot
			// Should divide 
			ScanResult<String> scanResult = jedisInstance.scan(0);
            List<String> result = scanResult.getResult();
            int cursor = scanResult.getCursor();
            keys.addAll(result);

            while(cursor > 0 && keys.size() < 100000){                    
                    //scanResult = jedisInstance.scan(cursor);
                    scanResult = jedisInstance.scan(cursor, params);
                    result = scanResult.getResult();
                    keys.addAll(result);
                    cursor = scanResult.getCursor();
            }
            
            totalKVs = keys.size();
            result.clear();
            //System.out.println("totalKVs = " + totalKVs);
			
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
		// Can be improved by processing non-hash records
		public boolean nextKeyValue() throws IOException, InterruptedException {			
			String currentHashKey;
			
			if(keys != null && !keys.isEmpty() && keys.size() > 0){
				currentHashKey = keys.get(0);
				//System.out.println("nextKeyValue() = " + currentHashKey);
				
				while(!(jedisInstance.type(currentHashKey)).equalsIgnoreCase("hash")){
					keys.remove(currentHashKey);
					
					if(!keys.isEmpty())
						currentHashKey = keys.get(0);
					else
						return false;
					//System.out.println("### nextKeyValue() = " + currentHashKey);
				}
				 				
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
			
			return false;
		}

		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		public float getProgress() throws IOException, InterruptedException {
			//System.out.println("RedisHashRecordReader::getProgress");
			return processedKVs / totalKVs;
		}

		public void close() throws IOException {
			jedisInstance.close();
		}

	} // end RedisHashRecordReader

} // end RedisHashInputFormat
