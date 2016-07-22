package com.booking.validation;

import com.mysql.jdbc.Blob;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created by lezhong on 7/14/16.
 */

public class Main {
    //  Database credentials
    static final String USER = "hadoop_admin";
    static final String PASS = "XyQxKluvC5p.g";
    private static String sql;
    private static ResultSet rs;
    private static ArrayList<String> tableList = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        try {
            // STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            // STEP 3: Open a connection
            System.out.println("Connecting to database...");
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser(USER);
            dataSource.setPassword(PASS);
            dataSource.setServerName("ha101av1rdb-01.ams4.prod.booking.com");
            conn = dataSource.getConnection();

            // STEP 4: Execute a query
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            sql = "use av;";
            stmt.executeQuery(sql);
            sql = "show tables";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                tableList.add(rs.getNString(1));
            }
            rs.close();

            for (String table: tableList) {
                sql = String.format("SELECT * FROM %s LIMIT 1 OFFSET 0;", table);
                ResultSet rs = stmt.executeQuery(sql);

                // STEP 5: Extract data from result set
                while (rs.next()) {
                    // Retrieve by column name
                    String last = rs.getString(1);
                    // Display values
                    System.out.print(String.format("%s -> %s\n", table, last));
                }
                // STEP 6: Clean-up environment
                rs.close();
            }
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            // Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            // finally block used to close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                // Do something
            } // nothing we can do
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally try
        } // end try
        System.out.println("Goodbye!");
    } // end main
}
