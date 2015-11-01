/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.tests.sqlite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.frontier.DocIDServer;

/**
 * 
 *
 * @author minkuan
 * @version $Id: SqliteUtil.java, v 0.1 Nov 1, 2015 12:55:43 PM minkuan Exp $
 */
public class SqliteUtil {

  /** LOGGER */
  private static final Logger            LOGGER        = LoggerFactory.getLogger(DocIDServer.class);

  /** SQLITE driver */
  private static final String            SQLITE_DRIVER = "org.sqlite.JDBC";

  private static Map<String, Connection> connMap       = new HashMap<String, Connection>();

  /**
   * @throws ClassNotFoundException driver not found
   * 
   */
  public static void ensureSqliteDriver() throws ClassNotFoundException {

    Class.forName(SQLITE_DRIVER);

  }

  /**
   * 
   * @param dbFullName db full name
   * @param autoCommit auto commit
   * @return db connection
   * @throws ClassNotFoundException driver not found
   * @throws SQLException sql ex
   */
  public static Connection ensureDbConn(String dbFullName, boolean autoCommit)
                                                                              throws ClassNotFoundException,
                                                                              SQLException {
    Connection conn = DriverManager.getConnection(dbFullName);
    assertNotNull(conn);
    connMap.put(dbFullName, conn);

    conn.setAutoCommit(autoCommit);
    return conn;
  }

  /**
   * 
   * @param conn db connection
   * @param tableCreateClause table crate clause
   * @throws ClassNotFoundException driver not found
   * @throws SQLException sql ex
   */
  public static void ensureTableExists(Connection conn, String tableCreateClause)
                                                                                 throws ClassNotFoundException,
                                                                                 SQLException {

    Statement stmt = conn.createStatement();

    Assert.assertTrue(stmt.executeUpdate(tableCreateClause) <= 1);
    stmt.close();
  }

  /**
   * 
   * @param dbFilename db file name
   */
  public static void ensureDbFileExists(String dbFilename) {
    File file = new File(dbFilename);
    assertTrue(file.getAbsolutePath(), file.exists());
  }

  /**
   * 
   * @param dbFilename db file name
   */
  public static void deleteDbFileIfExists(String dbFilename) {
    File file = new File(dbFilename);
    if (file.exists()) {
      file.delete();
      if (LOGGER.isInfoEnabled()) LOGGER.info("{} deleted", file.getAbsolutePath());
    }
  }

  /**
   * 
   * @param docsDbFullname
   * @return
   */
  public static Connection getConnection(String docsDbFullname) {
    Connection conn = connMap.get(docsDbFullname);
    Assert.assertNotNull(docsDbFullname, conn);
    return conn;
  }

}
