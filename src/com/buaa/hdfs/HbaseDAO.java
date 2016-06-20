 package com.buaa.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseDAO {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","192.168.25.205:2181,192.168.25.206:2181,192.168.25.207:2181" );
		HBaseAdmin admin = new HBaseAdmin(conf);
		String name = String.valueOf("nvshen");
		HTableDescriptor desc = new HTableDescriptor(name);
		HColumnDescriptor base_info = new HColumnDescriptor("base_info");
		HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
		base_info.setMaxVersions(5);
		desc.addFamily(base_info);
		desc.addFamily(extra_info);
		admin.createTable(desc);
	}

}
