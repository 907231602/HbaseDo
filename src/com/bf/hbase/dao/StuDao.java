package com.bf.hbase.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import com.bf.hbase.base.HbaseBaseDao;
import com.bf.hbase.common.HbaseConstants;

public class StuDao extends HbaseBaseDao {
	
	public void addStu(){
		try {
			HTable table=new HTable(conf, toBytes(HbaseConstants.TABLE_NAME));
			Put puts=new Put(toBytes("rk002"));
			puts.add(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_NAME), toBytes("战鼓"));
			puts.add(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_SEX), toBytes("man"));
			puts.add(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_AGE), toBytes("31"));
			
			table.put(puts);
			table.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void getStu(){
		try {
			HTable table=new HTable(conf, toBytes(HbaseConstants.TABLE_NAME));
			Get get=new Get(toBytes("rk001"));
			Result result= table.get(get);
		 byte names[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_NAME));
		 byte sexs[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_SEX));
		 byte ages[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_AGE));
			System.out.println(toByteToString(names)+"\t"+toByteToString(sexs)+"\t"+toByteToString(ages));
			table.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void scanStu(){
		try {
			HTable table=new HTable(conf, toBytes(HbaseConstants.TABLE_NAME));
			Scan scan=new Scan();
			ResultScanner rs= table.getScanner(scan);
			for (Result result : rs) {
				 byte names[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_NAME));
				 byte sexs[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_SEX));
				 byte ages[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_AGE));
				 System.out.println(toByteToString(names)+"\t"+toByteToString(sexs)+"\t"+toByteToString(ages));
					
			}
			
			table.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void scanFilterStu(){
		try {
			HTable table=new HTable(conf, toBytes(HbaseConstants.TABLE_NAME));
			Scan scan=new Scan();
		//	RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("rk"));
	
		//	RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(toBytes("rk001")));
		//	scan.setFilter(rowFilter);
			
			//多个过滤器用FilterList
			/*SingleColumnValueFilter scf = new SingleColumnValueFilter(toBytes(HbaseConstants.FAMALY),
					toBytes(HbaseConstants.COLUMN_AGE), CompareOp.EQUAL, new SubstringComparator("2"));

			SingleColumnValueFilter scf1 = new SingleColumnValueFilter(toBytes(HbaseConstants.FAMALY),
					toBytes(HbaseConstants.COLUMN_SEX), CompareOp.EQUAL, new SubstringComparator("man"));
			FilterList filterList=new FilterList();
			filterList.addFilter(scf);
			filterList.addFilter(scf1);
			
			scan.setFilter(filterList);*/
			
			//不包含要查寻的内容
			/*SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(toBytes(HbaseConstants.FAMALY),
					toBytes(HbaseConstants.COLUMN_SEX), CompareOp.EQUAL, new SubstringComparator("nan"));

			scan.setFilter(filter);*/
			
			//随机获取20%的数据
			/*RandomRowFilter filter=new RandomRowFilter((float)0.2);
			scan.setFilter(filter);*/

			//包含所查询的最后一条
			/*scan.setStartRow(toBytes("rk001"));
			//scan.setStopRow(toBytes("rk006"));
			InclusiveStopFilter filter=new InclusiveStopFilter(toBytes("rk002"));			
			scan.setFilter(filter);*/

			//限制查询的列
			/*Filter ccf = new ColumnCountGetFilter(3);			
			scan.setFilter(ccf);*/

			/*Filter ccf = new ValueFilter(CompareOp.EQUAL,new SubstringComparator("nan"));			
			scan.setFilter(ccf);*/
			
			
			Filter ccf = new ValueFilter(CompareOp.EQUAL,new SubstringComparator("ma"));			
			scan.setFilter(ccf);

			//查询所包含的前缀的内容
			/*PrefixFilter prefixFilter=new PrefixFilter(toBytes("rk"));
			scan.setFilter(prefixFilter);*/
			
			
			ResultScanner rs= table.getScanner(scan);
			for (Result result : rs) {
				 byte names[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_NAME));
				 byte sexs[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_SEX));
				 byte ages[]=	result.getValue(toBytes(HbaseConstants.FAMALY), toBytes(HbaseConstants.COLUMN_AGE));
				 System.out.println(toByteToString(names)+"\t"+toByteToString(sexs)+"\t"+toByteToString(ages));
					
			}
			
			table.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	//删除一行
	public void deleteStu(){
		try {
			HTable table=new HTable(conf, toBytes(HbaseConstants.TABLE_NAME));
			Delete delete=new Delete(toBytes("rk001"));
			table.delete(delete);
			
			table.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
