package com.boxple.redoop;

import java.io.*;
import java.util.*;
import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Twitter {
	
	// MAPPER
	public static class TwitMapper extends Mapper<LongWritable, Text, DateWordPair, IntWritable>{
	
		private JSONParser parser = new JSONParser();
		private StringTokenizer itr;
		
		private Text wordTxt = new Text();
		private IntWritable one = new IntWritable(1);
		private DateKey dateKey = new DateKey();
		private DateWordPair dateWordPair = new DateWordPair();

//		private final RedisPreCombiner<DateWordPair, IntWritable> combiner = new RedisPreCombiner<DateWordPair, IntWritable>(
//				new CombiningFunction<IntWritable>() {
//					@Override
//					public IntWritable combine(IntWritable value1, IntWritable value2) {
//						value1.set(value1.get() + value2.get());
//						return value1;
//					}
//		});
		
		// In-Node Combiner Variables
		private int port = 7003;
		private int minThreshold = 5;
		
		private final CombinerNode<DateWordPair, IntWritable> INCCombiner = new CombinerNode<DateWordPair, IntWritable>(
		new CombiningFunction<IntWritable>() {
			@Override
			public IntWritable combine(IntWritable value1, IntWritable value2) {
				value1.set(value1.get() + value2.get());
				return value1;
			}
		}, port, minThreshold);

//		private final CombinerPre<DateWordPair, IntWritable> IMCCombiner = new CombinerPre<DateWordPair, IntWritable>(
//		new CombiningFunction<IntWritable>() {
//			@Override
//			public IntWritable combine(IntWritable value1, IntWritable value2) {
//				value1.set(value1.get() + value2.get());
//				return value1;
//			}
//		});
		
	   @Override
	    public void setup(Context context) throws IOException,
	            InterruptedException {
		   
	    	//combiner.setPort(port);
		    //IMCCombiner.setContext(context);
		    //INCCombiner.setPort(port);
		   INCCombiner.setMapperStart(context.getTaskAttemptID().getTaskID().getId());
	    }
		   
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			try{
				JSONObject twit = (JSONObject) parser.parse(value.toString());

				//String tweetId = twit.get("id").toString();
				String msgStr = twit.get("text").toString();
				//String oriTweetId = twit.optString("oriTweetId", null);
				String dateStr = twit.get("date").toString();
				//String userId = twit.get("user").toString();

				// {"id":-1000001754,"text":"&lt;¼Õ¹Ù´Ú »ï±¹Áö2&gt;¸¦ ÇÃ·¹ÀÌ ÇØº¸ÀÚ! ÀÌÁ¦ ¸· ¿©Á¤¿¡ ¿Ã¶ú¾î. ³ª¿Í ÇÔ²²ÇÏÀÚ! ³» ÃßÃµÀÎ ÄÚµå¡°Yaho12¡±¸¦ ÀÔ·ÂÇÏ¸é ¸Å¿ì ÁÁÀº ¼±¹°À» ¹ÞÀ» ¼ö ÀÖ¾î! http://t.co/yvTmOut1SP","oriTweetId":null,"date":"Thu Mar 28 12:47:10 +0000 2013","user":1128677209}
				String urlPattern = "\\(?\\b(http://|www[.])[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]";
				String rtPattern = "(@-?[0-9]{6,14})\\s?";
				String repeatPattern = "[¤¡-¤¾¤¿-¤Ó]+";
				String speicalPattern = "[~`!@#$%^&*()\\-_={}\\[\\]\\+\\|,.:\\?\"><\"]";
				String quotePattern = "\\s'";
				String ltrtPattern = "(lt;|gt;)";
				String langPattern = "([^'\\w°¡-ÆR]+)";
				String spacePattern = "\\s{2,}";

				msgStr = msgStr.replaceAll("\n"," ");	
				msgStr = msgStr.replaceAll(urlPattern, "");
				msgStr = msgStr.replaceAll(rtPattern, "");
				msgStr = msgStr.replaceAll(speicalPattern, " ");
				msgStr = msgStr.replaceAll(ltrtPattern, " ");
				msgStr = msgStr.replaceAll(langPattern, " ");
				msgStr = msgStr.replaceAll(repeatPattern, " ");
				msgStr = msgStr.replaceAll(quotePattern, " ");
				msgStr = msgStr.replaceAll(spacePattern, " ");
				msgStr = msgStr.trim();

				Calendar cal = Calendar.getInstance();
				Date date = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse(dateStr);
				cal.setTime(date);
				dateKey = new DateKey(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH));

				dateWordPair.setDate(dateKey);		
				//if(oriTweetId.isNull()) reporter.incrCounter(TwitRankerCounters.total_retweets, 1);
								
				itr = new StringTokenizer(msgStr);
				while (itr.hasMoreTokens()) {
					wordTxt.set(itr.nextToken());
					dateWordPair.setWord(wordTxt);
					
					//context.write(dateWordPair, one);
					//IMCCombiner.write(dateWordPair, one, context);
					INCCombiner.write(dateWordPair, one, context);
				}								
			}catch(Exception e){
				//System.out.println(e.toString());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//combiner.close();
			//IMCCombiner.flush(context);
			INCCombiner.flush(context);
		}
	}
	

	// REDUCER
	public static class TwitReducer extends Reducer<DateWordPair, IntWritable, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> multipleOutputs;
		private IntWritable outputSum = new IntWritable();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
		}

		@Override
	    public void reduce(DateWordPair key, Iterable<IntWritable> values, Context output)
			throws IOException, InterruptedException {
			
			//System.out.println("reducer = " + key.getSecond());
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}			
			outputSum.set(sum);
			
			//output.write(key.getSecond(), outputSum);
			if(sum > 5)
				multipleOutputs.write("twitByDate", key.getSecond(), outputSum, (key.getFirst()).toString());
		}

		@Override
        public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
}
