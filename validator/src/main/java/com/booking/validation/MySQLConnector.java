package com.booking.validation;

import com.mysql.jdbc.exceptions.MySQLDataException;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by lezhong on 8/10/16.
 */

public class MySQLConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConnector.class);
    static Connection conn;
    static Statement stmt;

    MySQLConnector(String user, String pass) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            LOGGER.info("Connecting to database...");
            MysqlDataSource dataSource = new MysqlDataSource();
            dataSource.setUser(user);
            dataSource.setPassword(pass);
            dataSource.setServerName("ha101av1rdb-01.ams4.prod.booking.com");
            conn = dataSource.getConnection();

            // STEP 4: Execute a query
            LOGGER.info("Creating statement...");
        } catch (Exception se) {
            // Handle errors for JDBC
            se.printStackTrace();
        }
    }

    static ResultSet executeSQL(String sql) {
        try {
            stmt = conn.createStatement();
            ResultSet rst = stmt.executeQuery(sql);
            return rst;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    static class ColumnTypes {
        String dataType;
        String columnType;
        String charSet;
        String colation;
    }

    public static ArrayList<ColumnTypes> getColumnTypes(String db, String table) {
        String sql = String.format("SELECT "
                + "COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME "
                + "FROM "
                + "INFORMATION_SCHEMA.COLUMNS "
                + "WHERE "
                + "TABLE_SCHEMA = '%s' "
                + "AND "
                + "TABLE_NAME = '%s'", db, table);

        System.out.println(sql);

        ResultSet rst = executeSQL(sql);

        ArrayList<ColumnTypes> result = new ArrayList<>();
        try {
            while (rst.next()) {
                ColumnTypes col = new ColumnTypes();
                col.dataType = rst.getString("DATA_TYPE");
                col.columnType = rst.getString("COLUMN_TYPE");
                col.charSet = rst.getString("CHARACTER_SET_NAME");
                col.colation = rst.getString("COLLATION_NAME");
                result.add(col);
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return result;
    }
}
