package spider2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.*;


public class MySQLDemo {
	// MySQL 8.0 以下版本 - JDBC 驱动名及数据库 URL
	// static fianal String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	// static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB";

	// MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	// 输入数据库地址
	static final String DB_URL = "jdbc:mysql://localhost:3306/spider?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";

	// 数据库的用户名与密码，需要根据自己的设置
	static final String USER = "root";
	static final String PASS = "775735897";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		Statement stmt1 = null;
		try {
			// 注册 JDBC 驱动
			Class.forName(JDBC_DRIVER);

			// 打开链接
			System.out.println("连接数据库...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);

			// 执行查询
			System.out.println(" 实例化Statement对象...");
			stmt = conn.createStatement();
			stmt1 = conn.createStatement();
			String sql1 = "SELECT company_code FROM detail WHERE company_code=300009";
			ResultSet rs1 = stmt.executeQuery(sql1);
			// 展开结果集数据库
			while (rs1.next()) {
				String company_code = rs1.getString("company_code");
				System.out.print("公司编码: " + company_code);
				//写文件
				File f = new File("/Users/natsukakusunoki/Desktop/spider/detail/"+company_code+".csv");
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "gbk"));
				writer.write("公司名称,公司代码,公司网址,时间,提问,回答,问题网址,提问者,提问平台"+"\n");
				
				String sql2 = "SELECT company_name,company_code,company_http,time,"+
				"question,answer,question_http,question_people,question_plat FROM crawl3 WHERE company_code="
						+ company_code ;
				ResultSet rs2 = stmt1.executeQuery(sql2);
				//for(int i=0;i<2;i++) {
				while (rs2.next()) {
					String company_name = rs2.getString("company_name");
					String company_http = rs2.getString("company_http");
					String time = rs2.getString("time");
					String question = rs2.getString("question").replaceAll("\n|\r","").replaceAll(",","，");
					System.out.print("公司名称: " + question);
					String answer = rs2.getString("answer").replaceAll("\n|\r","").replaceAll(",","，");
					String question_http = rs2.getString("question_http");
					String question_people = rs2.getString("question_people");
					String question_plat = rs2.getString("question_plat");
					writer.write(company_name+","+"\t"+company_code+","+company_http+","+time+","+
							question+","+answer+","+question_http+","+question_people+","+question_plat+"\n");
					//writer.write(answer+"\n");
				    System.out.println("写入成功!!");
					System.out.print("公司名称: " + company_name);
					
				}
				System.out.print("\n");
				rs2.close();
				writer.flush();
				writer.close();
			}
			// 完成后关闭
			rs1.close();
			stmt.close();
			stmt1.close();
			conn.close();
		} catch (SQLException se) {
			// 处理 JDBC 错误
			se.printStackTrace();
		} catch (Exception e) {
			// 处理 Class.forName 错误
			e.printStackTrace();
		} finally {
			// 关闭资源
			try {
				if (stmt != null||stmt1 != null)
					stmt.close();
					stmt1.close();
			} catch (SQLException se2) {
			} // 什么都不做
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("Goodbye!");
	}
}
