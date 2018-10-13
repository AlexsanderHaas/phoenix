package main;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class main_start {
	
	private static main_start go_st;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		go_st = new main_start();
		
		go_st.m_select();
		
	}
	
	public void m_select() {
		
		String lv_table = "BRO_LOG";
		Map<String, String> map = new HashMap<String, String>();
		map.put("zkUrl", "localhost:2181");
		map.put("table", lv_table);
		
		SparkConf lv_conf = new SparkConf().setMaster("local[2]").setAppName("SelectLog");
		
		SparkContext lv_context = new SparkContext(lv_conf);
		
		SparkSession lv_session = new SparkSession(lv_context);
		
		Dataset<Row> lv_df = lv_session.sqlContext().load("org.apache.phoenix.spark", map);
		
		lv_df.registerTempTable(lv_table);
		 
				 
		Dataset<Row> lv_result = lv_df.sparkSession().sql(" SELECT * FROM BRO_LOG"
				+ " WHERE pk = '192.168.1.105-ClAP1m4Ohcjo9rLS95' ");
		
		lv_result.show();
		
	}
	
}
















