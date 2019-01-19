package com.madoverdata;

import java.sql.*;

/**
 * Use this class to connect with HiveServer2 using a username and pasword authentication.
 */
public class HiveEmployeeTests {
	//private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000, ";
	//private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/default;user=hive_user;password=hive_password";
	private static final String CONNECTION_URL = "jdbc:hive2://localhost:10000/default;user=williamgordon";
	private static String JDBCDriver = "org.apache.hive.jdbc.HiveDriver";
	private static Connection con = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;
	
	private static void createEmployee() throws SQLException, Exception {
		System.out.println("-------------------------");
		System.out.println("Executing create...");
		
		/*
		stmt.execute("CREATE TABLE IF NOT EXISTS "
		         +" employee ( id String, name String, dept String)"
		         +" COMMENT 'Employee details'"
		         +" ROW FORMAT DELIMITED"
		         +" FIELDS TERMINATED BY '\t'"
		         +" LINES TERMINATED BY '\n'"
		         +" STORED AS TEXTFILE");
		*/
		stmt.execute("CREATE TABLE IF NOT EXISTS "
		         +" employee ( id String, name String, dept String)"
		         +" COMMENT 'Employee details'"
		         +" ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'");
		
		System.out.println(" Table employee created.");
		System.out.println("-----------C--------------");
	}
	
	private static void loadEmployee() throws SQLException, Exception {
		System.out.println("-------------------------");
		Statement stmt = con.createStatement();
	      
	      // execute statement
	      //stmt.executeQuery("LOAD DATA LOCAL INPATH '/Users/Shared/workspace1/Hive-JDBC-tester/employee.txt'" + "OVERWRITE INTO TABLE employee;");
		  stmt.execute("LOAD DATA LOCAL INPATH '/Users/Shared/workspace1/Hive-JDBC-tester/employee.csv' OVERWRITE INTO TABLE employee");
	      System.out.println("Load Data into employee successful");
	      System.out.println("------------L-------------");
	}
	
	private static void queryEmployee(String query) throws SQLException, Exception {
		System.out.println("-------------------------");
		System.out.println("Executing query...");
		rs = stmt.executeQuery(query);
		System.out.println("Results:");
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
		System.out.println("-----------Q--------------");
	}

	public static void main(String[] args) {
		System.out.println("-------------------------");
		String query = "SELECT * from employee";
		try {
			Class.forName(JDBCDriver);
			System.out.println("Getting connection...");
			con = DriverManager.getConnection(CONNECTION_URL);
			//con.setUsername("hive_user");
	        //con.setPassword(env.getProperty("hive_password"));
			System.out.println("Got connection.");
			stmt = con.createStatement();
			createEmployee();
			loadEmployee();
			queryEmployee(query);
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
			System.out.println("------------FIN-------------");
		}
	}
}
