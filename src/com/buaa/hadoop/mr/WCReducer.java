package com.buaa.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	protected void reduce(Text text, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long count =0;
		for(LongWritable value:values){
			count+=value.get();
		}
		context.write(text, new LongWritable(count));
	}
	

}
