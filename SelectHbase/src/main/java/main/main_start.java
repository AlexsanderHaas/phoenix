package main;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

public class main_start {
	
	private static main_start go_st;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		go_st = new main_start();
		
		go_st.m_select();
		
	}
	
	public void m_select() {				
		
		String lv_table = "JSON6";//"BRO_LOG";
		Map<String, String> map = new HashMap<String, String>();
		map.put("zkUrl", "localhost:2181");
		map.put("hbase.zookeeper.quorum", "master");
		map.put("table", lv_table);
		
		SparkConf lv_conf = new SparkConf().setMaster("local[2]").setAppName("SelectLog");
					
		SparkContext lv_context = new SparkContext(lv_conf);
		
		SparkSession lv_session = new SparkSession(lv_context);
		
		//Dataset<Row> lv_df = lv_session.sqlContext().load("org.apache.phoenix.spark", map);
		
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		Dataset<Row> lv_df = lv_session.sqlContext().read()
							 .format("org.apache.phoenix.spark")
							 .options(map)
							 .load();
		
		lv_df.registerTempTable(lv_table);
		 
		//lv_df.printSchema();
		
		//lv_df.groupBy("query").count().show();
		
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		/*lv_df.filter(col("query").notEqual("www.tvgrenal.com.br"))
		.groupBy("id_orig_h","query").count().show();*/
				 
		/*Dataset<Row> lv_result = lv_df.sparkSession().sql(" SELECT * FROM BRO_LOG"
				+ " WHERE pk = '192.168.1.105-ClAP1m4Ohcjo9rLS95' ");
		
		lv_result.show();*/
		
		
		Dataset<Row> lv_conn;
		Dataset<Row> lv_dns;
		Dataset<Row> lv_result;
		Dataset<Row> lv_join;
		
		lv_conn = lv_df.filter(col("log").equalTo("conn"));
				  //.filter(col("service").equalTo("dns"));
		
		lv_dns = lv_df.filter(col("log").equalTo("dns"))
				 .filter(col("query").isNotNull())
				 .filter(col("query").equalTo("simone-pc"));
						
		
		/*lv_result = lv_dns.join(lv_conn, lv_dns.col("uid").equalTo(lv_conn.col("uid")))
					//.groupBy(lv_conn.col("uid"), lv_dns.col("query"))					
					.groupBy(lv_conn.col("uid"))					
					.count();
		*/		
		
		/*lv_result.sort(lv_result.col("count").desc())
				 .show();*/
		//sabe quantas vezes a uma conexão acessou tal coisa usando filtro entre dois dataset
		/*lv_result = lv_dns.join(lv_conn, lv_dns.col("uid").equalTo(lv_conn.col("uid")))				
				   .select(lv_dns.col("uid"),lv_dns.col("query"))
				   .groupBy(lv_dns.col("uid"),lv_dns.col("query"))
				   .count();
				
		
		lv_result.sort(lv_result.col("count").desc(), lv_result.col("uid"),lv_result.col("query"))
		.show(500);*/
		
		
		lv_result = lv_dns.groupBy("query").count().sort("count");
		
		lv_result.sort(lv_result.col("count").desc()).show();
		
		lv_dns.sort("uid").show();
		
		//lv_join = lv_dns //Pega do DNS so os que existem no CONN
				    //.join(lv_conn, lv_dns.col("uid").equalTo(lv_conn.col("uid"))).show();//fazer pegar do conn e nao do dns
		
		
		lv_join = lv_conn		//Pega o que tem no CONN conforme os dados do DNS		  
				  .join(lv_dns, lv_conn.col("uid").equalTo(lv_dns.col("uid")),"left");//fazer pegar do conn e nao do dns
		
		/*lv_join = lv_dns //Traz o que tem no DNS mas não tem no CONN				  				  
				  .join(lv_conn, lv_dns.col("uid").equalTo(lv_conn.col("uid")),"leftanti");//fazer pegar do conn e nao do dns
*/		
		lv_join.sort(lv_conn.col("uid")).show();
		
		
		//System.out.println("\nNumero de linhas de GERAL:\t"+lv_df.count());
		System.out.println("\nNumero de linhas de CONN:\t"+lv_conn.count());
		System.out.println("\nNumero de linhas de DNS:\t"+lv_dns.count());
		System.out.println("\nNumero de linhas de JOIN:\t"+lv_join.count());
		
		
		
	}
	
}
















