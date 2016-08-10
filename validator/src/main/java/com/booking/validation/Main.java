package com.booking.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


/**
 * Created by lezhong on 7/14/16.
 */


public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static class Config {
        String host;
        String table;
        String hbaseTable;
        HashMap<String, Boolean> tests;

        Config() {
            host = "";
            table = "";
            hbaseTable = "";
            tests = new HashMap<>();
        }
    }

    static Config get_config(String dbName) {
        Config ans = new Config();
        switch (dbName) {
            case "rescore": {
                ans.host = "ha101rescorerdb-01";
                ans.table = "B_RoomReservation";
                ans.hbaseTable = "rescore:b_roomreservation";
                ans.tests.put("version_count", false);
                ans.tests.put("cell_values", true);
                break;
            }
            case "bp": {
                ans.host = "ha101bprdb-01";
                ans.table = "B_Hotel";
                ans.hbaseTable = "bp:b_hotel";
                ans.tests.put("version_count", false);
                ans.tests.put("cell_values", true);
                break;
            }
            case "dw": {
                ans.host = "ha101dwrdb-01";
                ans.table = "Reservation";
                ans.hbaseTable = "replicator_dw:reservation";
                ans.tests.put("version_count", false);
                ans.tests.put("cell_values", true);
                break;
            }
            case "av": {
                ans.host = "ha101av1rdb-01";
                ans.table = "Availability_201304";
                ans.hbaseTable = "av:availability_201304";
                ans.tests.put("version_count", false);
                ans.tests.put("cell_values", true);
                break;
            }
            default: {
            }
        }
        return ans;
    }

    public static void main(String[] args) throws Exception {
        String dbName = "av";
        Config dbConfig = get_config(dbName);
        String restserverHost = "hb111toolserver-01"; // TODO: move to config
        String dataSource = String.format("dbi:mysql:%s;host=%s", dbName, dbConfig.host);
        String dataSourceInfo = String.format("dbi:mysql:information_schema;host=%s", dbConfig.host);
        String username = "hadoop_admin";
        System.out.println("Enter pwd please :");
        Scanner sc = new Scanner(System.in);
        String password = sc.next();

        MySQLConnector dbhInfo = new MySQLConnector(username, password);

        ArrayList<MySQLConnector.ColumnTypes> columnTypes = dbhInfo.getColumnTypes(dbName, dbConfig.table);

        // Value Match

        // Hbase Connection

        // get_ids

        // $chunks = partition 1, @ids;

        System.out.println("Total of ... ids from table $table will be tested, split info chunks.");

        int chunkNo = 0;

        HashMap<String, Integer> stats = new HashMap<>();
        stats.put("COLUMNS_PASS_TOTAL", 0);
        stats.put("COLUMNS_FAIL_TOTAL", 0);
        stats.put("IDS_PASS_TOTAL", 0);
        stats.put("IDS_FAIL_TOTAL", 0);

// get_tests();




//        MySQLGenerator mysqlGenerator = new MySQLGenerator();
//        HBaseGenerator hbaseGenerator = new HBaseGenerator();
//        ArrayList<String> tableList = mysqlGenerator.getTableList();
//        for (String table : tableList) {
//            hbaseGenerator.setTable(table);;
//            for (int offset = 0; ; offset++) {
//                ResultSet rsMySQL = mysqlGenerator.getResult(table, offset);
//                System.out.println(table);
//                // while (rsMySQL.next()) {
//                //    System.out.println(String.format("%s\n", rsMySQL.getString(1)));
//                // }
//                Result rsHBase = hbaseGenerator.getResult(String.format("row%d", offset));
//                System.out.println(String.format("%d\n", rsHBase.size()));
//            }
//        }

    } // end main
}
