package com.madoverdata;

import java.sql.*;

/**
 * Use this class to connect with HiveServer2 using a username and pasword authentication.
 */
public class Hive2User {
	private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000";
	static String JDBCDriver = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		//String query = "SELECT code, salary FROM default.sample_07 limit 5";
		String query = "SELECT * from employee";
		try {
			Class.forName(JDBCDriver);
			System.out.println("Getting connection...");
			con = DriverManager.getConnection(CONNECTION_URL);
			System.out.println("Got connection.");
			stmt = con.createStatement();
			System.out.println("Executing query...");
			rs = stmt.executeQuery(query);
			System.out.println("Results:");
			while (rs.next()) {
				//String code = rs.getString("code");
				//int salary = rs.getInt("salary");
				//System.out.printf("%s, %d%n", code, salary);
				System.out.println(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException se1) {
				se1.printStackTrace();
			}
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se2) {
				se2.printStackTrace();
			}
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException se4) {
				se4.printStackTrace();
			}
		}
	}
}
