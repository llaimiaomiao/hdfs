package com.buaa.miao.urlgg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutPutFormat<K, V> extends FileOutputFormat {
	private FileSystem fs;

	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		fs = FileSystem.get(new Configuration());
		FSDataOutputStream fsNomal = fs.create(new Path("/liuliang/outputIS"));
		FSDataOutputStream fsUnNomal = fs.create(new Path("/liuliang/outputNot"));
		return new MyRecordWriter(fsNomal, fsUnNomal);
	}

	public class MyRecordWriter extends RecordWriter<K, V> {
		
		FSDataOutputStream fsNomal =null;
		FSDataOutputStream fsUnNomal =null;
		
		public MyRecordWriter(FSDataOutputStream fsNomal, FSDataOutputStream fsUnNomal) {
			super();
			this.fsNomal = fsNomal;
			this.fsUnNomal = fsUnNomal;
		}

		@Override
		public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
				if(fsNomal!=null){
					fsNomal.close();
				}if(fsUnNomal!=null){
					fsUnNomal.close();
				}
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			if(key.toString().contains("ISNOTFOUND!!!")){
				fsUnNomal.write(key.toString().getBytes());
			}
			else{
				fsNomal.write(key.toString().getBytes());
			}

		}

	}

}
