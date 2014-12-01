package com.boxple.redoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateWordPair implements WritableComparable<DateWordPair>{

        private DateKey first = new DateKey();
        private Text second = new Text();

        public DateWordPair() {}

        public DateWordPair(DateKey t1, Text t2) {
                first = t1;
                second = t2;
        }
        
        public void setDate(DateKey t1) {
                first = t1;
        }
        
        public void setWord(Text t2) {
                second = t2;
        }

        public DateKey getFirst() {
                return first;
        }

        public Text getSecond() {
                return second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
                first.write(out);
                second.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
                if (first == null)
                        first = new DateKey();

                if (second == null)
                        second = new Text();

                first.readFields(in);
                second.readFields(in);
        }

        @Override
        public int hashCode() {
                return first.getYear() + first.getMonth() + first.getDay() + second.hashCode();
        }

        @Override
        public int compareTo(DateWordPair dw){
                DateKey k1 = first;
                DateKey k2 = dw.getFirst();

                if(k1.compareTo(k2) != 0)
                        return k1.compareTo(k2);
                else
                        return second.compareTo(dw.getSecond());
        }

        @Override
        public boolean equals(Object o) {
                if (o instanceof DateWordPair) {
                        DateWordPair dw = (DateWordPair) o;
                        return first.equals(dw.getFirst()) && second.equals(dw.getSecond());
                }
                return false;
        }

        @Override
        public String toString(){
                return first.toString() + "," + second.toString();
        }
        
        public void setDateWord(String str) throws ParseException{
        	String dateStr = str.split(",")[0];
        	String wordStr = str.split(",")[1];
        				
			Calendar cal = Calendar.getInstance();
			Date date = new SimpleDateFormat("yyyyMMdd").parse(dateStr);
			cal.setTime(date);
			DateKey dateKey = new DateKey(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH));
			
			setDate(dateKey);
			second.set(wordStr);
			
			//first = new DateKey(Integer.parseInt(date.substring(0, 4)), Integer.parseInt(date.substring(4, 6)), Integer.parseInt(date.substring(6)));						
        }


        public static class FirstComparator extends WritableComparator{         
                public FirstComparator(){
                        super(DateWordPair.class, true);
                }

                @SuppressWarnings("rawtypes")
				@Override
                public int compare(WritableComparable w1, WritableComparable w2){
                        DateWordPair dw1 = (DateWordPair) w1;
                        DateWordPair dw2 = (DateWordPair) w2;

                        DateKey k1 = dw1.getFirst();
                        DateKey k2 = dw2.getFirst();

                        return k1.compareTo(k2);
                }
        }
}
