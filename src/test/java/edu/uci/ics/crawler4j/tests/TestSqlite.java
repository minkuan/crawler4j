/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.tests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * 
 *
 * @author minkuan
 * @version $Id: TestSqlite.java, v 0.1 Nov 1, 2015 2:40:08 AM minkuan Exp $
 */
public class TestSqlite {
  /**
   * 
   * @param args
   */
  public static void main(String args[]) {
    //    Connection c = null;
    //    try {
    //      Class.forName("org.sqlite.JDBC");
    //      c = DriverManager.getConnection("jdbc:sqlite:test.db");
    //    } catch (Exception e) {
    //      System.err.println(e.getClass().getName() + ": " + e.getMessage());
    //      System.exit(0);
    //    }
    //    System.out.println("Opened database successfully");

    Connection c = null;
    Statement stmt = null;
    try {
      Class.forName("org.sqlite.JDBC");
      c = DriverManager.getConnection("jdbc:sqlite:test.db");
      System.out.println("Opened database successfully");

      stmt = c.createStatement();
      String sql = "CREATE TABLE COMPANY " + "(ID INT PRIMARY KEY     NOT NULL,"
                   + " NAME           TEXT    NOT NULL, " + " AGE            INT     NOT NULL, "
                   + " ADDRESS        CHAR(50), " + " SALARY         REAL)";
      stmt.executeUpdate(sql);
      stmt.close();
      c.close();
    } catch (Exception e) {
      System.err.println(e.getClass().getName() + ": " + e.getMessage());
      System.exit(0);
    }
    System.out.println("Table created successfully");
  }
}