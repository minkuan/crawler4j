/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.frontier;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *
 * @author minkuan
 * @version $Id: DocsTable.java, v 0.1 Nov 1, 2015 6:11:01 PM minkuan Exp $
 */
public class DocsTable {

  /** LOGGER */
  static final Logger        LOGGER         = LoggerFactory.getLogger(DocsTable.class);

  /** create clause */
  // TODO 待创建url索引
  public static final String CREATE_CLAUSE  = "CREATE TABLE IF NOT EXISTS %s ("
                                              + "ID INT PRIMARY KEY NOT NULL, "
                                              + "URL TEXT NOT NULL, " + "PARENT_ID INT NOT NULL, "
                                              + "PARENT_URL TEXT DEFAULT NULL, "
                                              + "DEPTH INT NOT NULL, "
                                              + "ANCHOR TEXT DEFAULT NULL, "
                                              + "STATE INT NOT NULL, "
                                              + "GMT_MODIFIED INT NOT NULL" + ")";

  /** id column */
  public static final String C1_ID          = "ID";

  /** url column */
  public static final String C2_URL         = "URL";

  /** parent id column */
  public static final String C3_PARENT_ID   = "PARENT_ID";

  /** parent url column */
  public static final String C4_PARENT_URL  = "PARENT_URL";

  /** depth column */
  public static final String C5_DEPTH       = "DEPTH";

  /**  */
  public static final String C6_ANCHOR      = "ANCHOR";

  /** state column */
  public static final String C7_STATE       = "STATE";

  /** gmtmodified column */
  public static final String C8_GMTMODIFIED = "GMT_MODIFIED";

  /**
   * 
   * @param url
   * @param conn
   * @param tableName
   * @return
   * @throws SQLException
   */
  public static final DocRecord queryByUrl(final String url, Connection conn, final String tableName)
                                                                                                     throws SQLException {
    assertTrue(format("tableName:%s,url:%s", tableName, url), isNotBlank(url) && conn != null
                                                              && tableName != null);

    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.createStatement();
      rs = stmt.executeQuery(String.format("SELECT * FROM %s WHERE URL='%s'", tableName, url));
      while (rs.next()) {
        return new DocRecord().fillId(rs.getInt(C1_ID)).fillUrl(rs.getString(C2_URL))
          .fillParentId(rs.getInt(C3_PARENT_ID)).fillParentUrl(rs.getString(C4_PARENT_URL))
          .fillDepth(rs.getShort(C5_DEPTH)).fillAnchor(rs.getString(C6_ANCHOR))
          .fillGmtModified(rs.getLong(C8_GMTMODIFIED));
      }
    } finally {
      if (rs != null) rs.close();
      stmt.close();
    }

    return null;

  }

  /**
   * 
   * @param conn db connection
   * @param doc doc record
   * @param tableName 
   * @throws SQLException SQL ex 
   */
  public static void insertDoc(final Connection conn, final DocRecord doc, final String tableName)
                                                                                                  throws SQLException {

    Assert.assertTrue(conn != null && doc != null);
    doc.assertValid();

    Statement stmt = null;
    StringBuffer sb = new StringBuffer();
    int rc = -1;
    try {
      stmt = conn.createStatement();

      sb.append("INSERT INTO ").append(tableName).append("(").append(C1_ID).append(",");
      sb.append(C2_URL).append(",");
      /* if (doc.getParentId() > 0) */sb.append(C3_PARENT_ID).append(",");
      if (StringUtils.isNotBlank(doc.getParentUrl())) sb.append(C4_PARENT_URL).append(",");
      /* if (doc.getDepth() > 0) */sb.append(C5_DEPTH).append(",");
      if (StringUtils.isNotBlank(doc.getAnchor())) sb.append(C6_ANCHOR).append(",");
      sb.append(C7_STATE).append(",");
      sb.append(C8_GMTMODIFIED);
      sb.append(") VALUES(");
      sb.append(doc.getId()).append(",'").append(doc.getUrl()).append("',");
      /* if (doc.getParentId() > 0) */sb.append(doc.getParentId()).append(",");
      if (StringUtils.isNotBlank(doc.getParentUrl()))
        sb.append("'").append(doc.getParentUrl()).append("',");
      /* if (doc.getDepth() > 0) */sb.append(doc.getDepth()).append(",");
      if (StringUtils.isNotBlank(doc.getAnchor()))
        sb.append("'").append(doc.getAnchor()).append("',");
      sb.append(DocState.INIT.getCode()).append(",");
      sb.append(System.currentTimeMillis());
      sb.append(")");

      rc = stmt.executeUpdate(sb.toString());
      assertEquals(1, rc);
    } catch (Exception e) {
      LOGGER.error("sql:" + sb.toString(), e);
      throw e;
    } finally {
      stmt.close();
    }
  }
}
