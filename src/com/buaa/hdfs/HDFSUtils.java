package com.buaa.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
public class HDFSUtils {
	private  FileSystem fs =null;
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://MiaoMiao20:9000/");
		fs = FileSystem.get(new URI("hdfs://MiaoMiao20:9000/"), conf, "hadoop");
	}
	@Test
	public void upload() throws IllegalArgumentException, IOException{
		
		FSDataOutputStream os = fs.create(new Path("hdfs://MiaoMiao20:9000/aa/qingshu.txt"));
		
		FileInputStream in = new FileInputStream("C:/eula.1028.txt");
		IOUtils.copy(in, os);
		
	}
	@Test
	public void upload2() throws IllegalArgumentException, IOException{
		
		fs.copyFromLocalFile(new Path("C:/eula.1028.txt"), new Path("hdfs://MiaoMiao20:9000/aa/qingshu2.txt"));
		
	}
	@Test
	public void downLoad() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(new Path("hdfs://MiaoMiao20:9000/aa/qingshu2.txt"),new Path("C:/biubiu.txt"));
		
	}
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException{
		
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("hdfs://MiaoMiao20:9000/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath().getName()+" "+(next.isDirectory()?"dir":"file"));
		}
		System.out.println("------------------------");
		FileStatus[] listStatus = fs.listStatus(new Path("hdfs://MiaoMiao20:9000/"));
		for (FileStatus status:listStatus) {
			System.out.println(status.getPath().getName()+" "+(status.isDirectory()?"dir":"file"));
		}	
	}
	@Test
	public void rm() throws IllegalArgumentException, IOException{
		fs.delete(new Path("hdfs://MiaoMiao20:9000/aa"), true);
		
	}
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://MiaoMiao20:9000/");
		FileSystem fs = FileSystem.get(new URI("hdfs://MiaoMiao20:9000/"), conf, "hadoop");
	}
}
