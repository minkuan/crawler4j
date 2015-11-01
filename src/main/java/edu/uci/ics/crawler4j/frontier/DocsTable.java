/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.frontier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 *
 * @author minkuan
 * @version $Id: DocsTable.java, v 0.1 Nov 1, 2015 6:11:01 PM minkuan Exp $
 */
public class DocsTable {

  /** create clause */
  public static final String CREATE_CLAUSE     = "CREATE TABLE IF NOT EXISTS DOCS ("
                                                 + "ID INT PRIMARY KEY NOT NULL, "
                                                 + "URL TEXT NOT NULL, "
                                                 + "PARENT_ID INT NOT NULL, "
                                                 + "PARENT_URL TEXT NOT NULL, "
                                                 + "DEPTH INT NOT NULL, " + "STATE INT NOT NULL, "
                                                 + "GMT_MODIFIED INT NOT NULL" + ")";

  /** id column */
  public static final String COL_ID_0          = "ID";

  /** url column */
  public static final String COL_URL_1         = "URL";

  /** parent id column */
  public static final String COL_PARENT_ID_2   = "PARENT_ID";

  /** parent url column */
  public static final String COL_PARENT_URL_3  = "PARENT_URL";

  /** depth column */
  public static final String COL_DEPTH_4       = "DEPTH";

  /**  */
  public static final String COL_ANCHOR_5      = "ANCHOR";

  /** state column */
  public static final String COL_STATE_6       = "STATE";

  /** gmtmodified column */
  public static final String COL_GMTMODIFIED_7 = "GMT_MODIFIED";

  /**
   * 
   * @param conn db connection
   * @param tableName table name
   * @return table records number
   * @throws SQLException SQL ex
   */
  public static final int queryCount(final Connection conn, String tableName) throws SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    int docCount = 0;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName));
      while (rs.next()) {
        docCount = rs.getInt(0);
        break;
      }
    } finally {
      rs.close();
      stmt.close();
    }
    return docCount;
  }

  /**
   * 
   * @param url
   * @param conn
   * @param tableName
   * @return
   * @throws SQLException
   */
  public static final DocRecord queryByUrl(final String url, Connection conn, Object tableName)
                                                                                               throws SQLException {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(String.format("SELECT %s,%s,%s,%s,%s FROM %s WHERE URL=%s", COL_ID_0,
        COL_URL_1, COL_PARENT_ID_2, COL_PARENT_URL_3, COL_DEPTH_4, tableName, COL_URL_1));
      while (rs.next()) {
        return new DocRecord().fillId(rs.getInt(0)).fillUrl(rs.getString(1))
          .fillParentId(rs.getInt(2)).fillParentUrl(rs.getString(3)).fillDepth(rs.getInt(4));
      }
    } finally {
      rs.close();
      stmt.close();
    }

    return null;

  }

  /**
   * 
   * @param conn db connection
   * @param doc doc record
   * @throws SQLException SQL ex 
   */
  public static void insertDoc(final Connection conn, final DocRecord doc) throws SQLException {

    assertTrue(doc.toString(),
      doc != null && doc.getId() > 0 && StringUtils.isNotBlank(doc.getUrl()));
    assertTrue(doc.toString(), ((doc.getParentId() > 0 && StringUtils
      .isNotBlank(doc.getParentUrl())) || (doc.getParentId() <= 0 && StringUtils.isBlank(doc
      .getParentUrl()))));

    Statement stmt = null;
    int rc = -1;
    try {
      stmt = conn.createStatement();

      StringBuffer sb = new StringBuffer();
      sb.append("INSERT INTO DOCS(").append(COL_ID_0).append(",");
      sb.append(COL_URL_1).append(",");
      if (doc.getParentId() > 0) sb.append(COL_PARENT_ID_2).append(",");
      if (StringUtils.isNotBlank(doc.getParentUrl())) sb.append(COL_PARENT_URL_3).append(",");
      if (doc.getDepth() > 0) sb.append(COL_DEPTH_4).append(",");
      if (StringUtils.isNotBlank(doc.getAnchor())) sb.append(COL_ANCHOR_5).append(",");
      sb.append(COL_STATE_6).append(",");
      sb.append(COL_GMTMODIFIED_7);
      sb.append(") VALUES(");
      sb.append(doc.getId()).append(",").append(doc.getUrl()).append(",");
      if (doc.getParentId() > 0) sb.append(doc.getParentId()).append(",");
      if (StringUtils.isNotBlank(doc.getParentUrl())) sb.append(doc.getParentUrl()).append(",");
      if (doc.getDepth() > 0) sb.append(doc.getDepth()).append(",");
      if (StringUtils.isNotBlank(doc.getAnchor())) sb.append(doc.getAnchor()).append(",");
      sb.append(DocState.INIT.getCode()).append(",");
      sb.append(System.currentTimeMillis());
      sb.append(")");

      rc = stmt.executeUpdate(sb.toString());
      assertEquals(1, rc);
    } finally {
      stmt.close();
    }
  }
}
