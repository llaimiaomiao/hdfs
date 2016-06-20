package com.buaa.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HAHdfs {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"), conf, "hadoop");
		fs.copyFromLocalFile(new Path("c:/miaomiao.txt"), new Path("hdfs://ns1/"));
		
	}

}
