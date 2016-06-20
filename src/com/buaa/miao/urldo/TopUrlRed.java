package com.buaa.miao.urldo;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopUrlRed extends Reducer<Text, FlowBean, Text, LongWritable>{
		private double gloableCount =0;
		private TreeMap<FlowBean, Text> treeMap = new TreeMap<>();
		@Override
		protected void reduce(Text text, Iterable<FlowBean> values,  Context context)
				throws IOException, InterruptedException {
			Text url = new Text(text.toString());
			long up_sum = 0;
			long d_sum = 0;
			for(FlowBean val:values){
				up_sum +=val.getUp_flow();
				d_sum += val.getD_flow();
			}
			FlowBean bean = new FlowBean(up_sum, d_sum);
			gloableCount +=bean.getSum_flow();
			treeMap.put(bean, url);
		}
		@Override
		protected void cleanup(Context context)
			throws IOException, InterruptedException {
			Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
			double tempCount = 0;
		for(Entry<FlowBean, Text> ent:entrySet){
			if(tempCount /gloableCount <0.8){
			context.write(ent.getValue(), new LongWritable(ent.getKey().getSum_flow()));
			tempCount += ent.getKey().getSum_flow();
			}
			else{
				return;
			}
		}
		}
}
