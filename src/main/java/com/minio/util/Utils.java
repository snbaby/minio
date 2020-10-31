package com.minio.util;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSONObject;
import com.github.drinkjava2.jdbpro.SqlItem;
import com.github.drinkjava2.jsqlbox.DB;

public class Utils {
	public static void consStr(List<SqlItem> sqlItemList, JSONObject jsb, String name) {
		if (jsb == null) {
			return;
		}
		Object obj = jsb.get(name);
		if (obj == null) {
			return;
		}
		
		if(StringUtils.isBlank(obj.toString())) {
			return;
		}

		try {
			sqlItemList.add(DB.notNull(name + ",", obj.toString()));
		} catch (Exception e) {
			// TODO: handle exception
			// 异常直接丢弃，不存入数据库
		}
	}
	
	public static void consInteger(List<SqlItem> sqlItemList, JSONObject jsb, String name) {
		if (jsb == null) {
			return;
		}
		Object obj = jsb.get(name);
		if (obj == null) {
			return;
		}

		try {
			if (StringUtils.isNotBlank(obj.toString())) {
				sqlItemList.add(DB.notNull(name + ",", Long.parseLong(obj.toString())));
			}
		} catch (Exception e) {
			// TODO: handle exception
			// 异常直接丢弃，不存入数据库
		}
	}

	public static void consDecimal(List<SqlItem> sqlItemList, JSONObject jsb, String name) {
		if (jsb == null) {
			return;
		}
		Object obj = jsb.get(name);
		if (obj == null) {
			return;
		}
		try {
			sqlItemList.add(DB.notNull(name + ",", Double.parseDouble(obj.toString())));
		} catch (Exception e) {
			// TODO: handle exception
			// 异常直接丢弃，不存入数据库
		}
	}

	public static void consDate(List<SqlItem> sqlItemList, JSONObject jsb, String name) {
		if (jsb == null) {
			return;
		}
		Object obj = jsb.get(name);
		if (obj == null) {
			return;
		}
		try {
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			sqlItemList.add(DB.notNull(name + ",", format.format(format.parse(obj.toString()))));
		} catch (Exception e) {
			// TODO: handle exception
			// 异常直接丢弃，不存入数据库
		}
	}

	public static boolean validate(JSONObject jsb) {
		if (jsb == null) {
			return false;
		}
		Object pkp_waybill_idObj = jsb.get("pkp_waybill_id");
		if (pkp_waybill_idObj == null) {
			return false;
		}

		if (!(pkp_waybill_idObj.getClass().getSimpleName().equals("Integer")||pkp_waybill_idObj.getClass().getSimpleName().equals("Long"))) {
			return false;
		}

		Object waybill_noObj = jsb.get("waybill_no");
		if (waybill_noObj == null) {
			return false;
		}

		if (StringUtils.isBlank(waybill_noObj.toString())) {
			return false;
		}

		return true;
	}

	public static void traverFile(File file, List<File> fileList) {
		if (file.isDirectory()) {
			for (File tempFile : file.listFiles()) {
				traverFile(tempFile, fileList);
			}
		} else {
			fileList.add(file);
		}
	}
}
