package com.bf.hbase.test;

import com.bf.hbase.dao.StuDao;

public class TestH {
	public static void main(String[] args) {
		StuDao stuDao=new StuDao();
		//stuDao.addStu();
		//stuDao.getStu();
		//stuDao.scanStu();
		//stuDao.scanFilterStu();
		stuDao.deleteStu();
		
	}
}
