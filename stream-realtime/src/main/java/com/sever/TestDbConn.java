package com.sever;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestDbConn {
    public static void main(String[] args) {
        try {
            Connection conn = DriverManager.getConnection("jdbc:sqlserver://192.168.200.101:1433;databaseName=test", "sa", "lz0918./");
            System.out.println("Connected: " + conn);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
