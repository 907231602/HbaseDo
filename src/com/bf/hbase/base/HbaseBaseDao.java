package com.bf.hbase.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseBaseDao {
	public Configuration conf=HBaseConfiguration.create();
	public HbaseBaseDao() {
		//¥Ê¥¢zookeeperµÿ÷∑
		conf.set("hbase.zookeeper.quorum", "yanjijun1:2181,yanjijun2:2181,yanjijun3:2181");
	}
	
	public byte[] toBytes(String info){
		return Bytes.toBytes(info);
	}
	
	public String toByteToString(byte[] context){
		
		return Bytes.toString(context);
	}
}
