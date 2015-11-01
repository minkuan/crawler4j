/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.tests.sqlite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 *
 * @author minkuan
 * @version $Id: TestSqlite.java, v 0.1 Nov 1, 2015 2:43:52 AM minkuan Exp $
 */
public class TestSqlite {

  /** DB full name. jdbc protocol can NOT be omitted, so that db existence can NOT be retrived through that of the db file. */
  private static final String DB_FULLNAME = "jdbc:sqlite:test.db";

  /** table name */
  private static final String TABLE_NAME  = "COMPANY";

  /** db name */
  private static final String DB_FILENAME = "test.db";

  /** the underlying connection */
  private Connection          conn        = null;

  /**
   * 
   * @throws ClassNotFoundException driver not found
   * @throws SQLException sql ex
   */
  @Before
  public void setUp() throws ClassNotFoundException, SQLException {
    SqliteUtil.ensureSqliteDriver();

    conn = SqliteUtil.ensureDbConn(DB_FULLNAME, false);

    SqliteUtil.ensureDbFileExists(DB_FILENAME);

    // ensureDbExists(DB_NAME);
    ensureTableExists();
  }

  /**
   * 
   * @param dbName db name
   * @throws SQLException sql ex
   */
  //  public void ensureDbExists(String dbName) throws SQLException {
  //    Statement stmt = null;
  //    ResultSet rs = null;
  //    try {
  //      stmt = conn.createStatement();
  //
  //      rs = stmt.executeQuery(format(DB_SELECT_CLAUSE_FMT, dbName));
  //      assertTrue(rs.next());
  //
  //    } finally {
  //      rs.close();
  //      stmt.close();
  //    }
  //  }

  /**
   * 
   * @throws ClassNotFoundException driver not found
   * @throws SQLException sql ex
   */
  public void ensureTableExists() throws ClassNotFoundException, SQLException {

    Statement stmt = conn.createStatement();

    String sql = String.format("CREATE TABLE IF NOT EXISTS %s " + "(ID INT PRIMARY KEY NOT NULL,"
                               + " NAME TEXT NOT NULL, " + " AGE INT NOT NULL, "
                               + " ADDRESS CHAR(50), " + " SALARY REAL)", TABLE_NAME);
    Assert.assertTrue(stmt.executeUpdate(sql) <= 1);
    stmt.close();
  }

  /**
   * 
   * @throws SQLException sql ex
   * @throws ClassNotFoundException driver not found
   */
  @Test
  public void testInsert() throws SQLException, ClassNotFoundException {

    Statement stmt = conn.createStatement();
    String sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
                 + "VALUES (1, 'Paul', 32, 'California', 20000.00 );";
    int rc = stmt.executeUpdate(sql);
    Assert.assertEquals(1, rc);

    sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
          + "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
    rc = stmt.executeUpdate(sql);
    Assert.assertEquals(1, rc);

    sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
          + "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
    rc = stmt.executeUpdate(sql);
    Assert.assertEquals(1, rc);

    sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
          + "VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 );";
    rc = stmt.executeUpdate(sql);
    Assert.assertEquals(1, rc);

    stmt.close();
    conn.commit();

  }

  /**
   * 
   * @throws ClassNotFoundException driver not found
   * @throws SQLException sql ex
   */
  @Test
  public void testSelect() throws ClassNotFoundException, SQLException {

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM %s;", TABLE_NAME));
    while (rs.next()) {
      int id = rs.getInt("id");
      String name = rs.getString("name");
      int age = rs.getInt("age");
      String address = rs.getString("address");
      float salary = rs.getFloat("salary");
      System.out.println("ID = " + id);
      System.out.println("NAME = " + name);
      System.out.println("AGE = " + age);
      System.out.println("ADDRESS = " + address);
      System.out.println("SALARY = " + salary);
      System.out.println();
    }
    rs.close();
    stmt.close();
  }

  /**
   * 
   * @throws SQLException sql ex
   * @throws ClassNotFoundException driver not found
   */
  @Test
  public void testUpdate() throws SQLException, ClassNotFoundException {

    Statement stmt = conn.createStatement();
    String sql = "UPDATE COMPANY set SALARY = 25000.00 where ID=1;";
    int rc = stmt.executeUpdate(sql);
    Assert.assertEquals(1, rc);
    conn.commit();

    ResultSet rs = stmt.executeQuery("SELECT * FROM COMPANY;");
    while (rs.next()) {
      int id = rs.getInt("id");
      String name = rs.getString("name");
      int age = rs.getInt("age");
      String address = rs.getString("address");
      float salary = rs.getFloat("salary");
      System.out.println("ID = " + id);
      System.out.println("NAME = " + name);
      System.out.println("AGE = " + age);
      System.out.println("ADDRESS = " + address);
      System.out.println("SALARY = " + salary);
      System.out.println();
    }
    rs.close();
    stmt.close();
  }

  /**
   * 
   * @throws SQLException sql ex
   * @throws ClassNotFoundException driver not found
   */
  @Test
  public void testDelete() throws SQLException, ClassNotFoundException {

    Statement stmt = conn.createStatement();
    String sql = "DELETE from COMPANY where ID=2;";
    int rc = stmt.executeUpdate(sql);
    Assert.assertTrue(rc <= 1);
    conn.commit();

    ResultSet rs = stmt.executeQuery("SELECT * FROM COMPANY;");
    while (rs.next()) {
      int id = rs.getInt("id");
      String name = rs.getString("name");
      int age = rs.getInt("age");
      String address = rs.getString("address");
      float salary = rs.getFloat("salary");
      System.out.println("ID = " + id);
      System.out.println("NAME = " + name);
      System.out.println("AGE = " + age);
      System.out.println("ADDRESS = " + address);
      System.out.println("SALARY = " + salary);
      System.out.println();
    }
    rs.close();
    stmt.close();
  }

  /**
   * 
   * @throws SQLException sql ex
   */
  protected void ensureDbClose() throws SQLException {
    if (conn != null) conn.close();
  }

  /**
   * 
   * @throws SQLException sql ex
   */
  @After
  public void tearDown() throws SQLException {
    ensureDbClose();
  }
}
