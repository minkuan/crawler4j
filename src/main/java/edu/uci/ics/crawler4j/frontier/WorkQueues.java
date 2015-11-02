/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package edu.uci.ics.crawler4j.frontier;

import static edu.uci.ics.crawler4j.frontier.DocsTable.CREATE_CLAUSE;
import static edu.uci.ics.crawler4j.tests.sqlite.SqliteUtil.ensureTableExists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;

import com.sleepycat.je.Transaction;

import edu.uci.ics.crawler4j.tests.sqlite.SqliteUtil;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar
 */
public class WorkQueues {
  /**  */
  //  private static final String WQ_DB_FILENAME = "pending_urls.db";
  /**  */
  //private static final String WQ_DB_FULLNAME = ;
  // private final Database           urlsDB;
  //  private final Environment        env;

  protected final boolean resumable;

  //  private final WebURLTupleBinding webURLBinding;

  protected final Object  mutex = new Object();

  public WorkQueues(/* Environment env, String dbName, */boolean resumable)
                                                                           throws ClassNotFoundException,
                                                                           SQLException {
    //    this.env = env;
    this.resumable = resumable;
    //    DatabaseConfig dbConfig = new DatabaseConfig();
    //    dbConfig.setAllowCreate(true);
    //    dbConfig.setTransactional(resumable);
    //    dbConfig.setDeferredWrite(!resumable);
    //    urlsDB = env.openDatabase(null, dbName, dbConfig);
    //    webURLBinding = new WebURLTupleBinding();

    if (!resumable) {
      SqliteUtil.deleteDbFileIfExists(getDbFilename());
    }
    SqliteUtil.ensureDbConn(getDbFullname(), resumable);
    SqliteUtil.ensureDbFileExists(getDbFilename());
    //    "CREATE TABLE IF NOT EXISTS %s " + "(ID INT PRIMARY KEY NOT NULL,"
    //    + " NAME TEXT NOT NULL, " + " AGE INT NOT NULL, "
    //    + " ADDRESS CHAR(50), " + " SALARY REAL)", TABLE_NAME
    ensureTableExists(SqliteUtil.getConnection(getDbFullname()),
      String.format(CREATE_CLAUSE, getTablename()));

    /* lastDocID = */SqliteUtil.queryCount(SqliteUtil.getConnection(getDbFullname()),
      getTablename());
    //    if (lastDocID > 0)
    //      if (LOGGER.isInfoEnabled())
    //        LOGGER.info("Loaded {} URLs that had been detected in previous crawl.", lastDocID);

  }

  public String getTablename() {
    return "workqueue";
  }

  public String getDbFullname() {
    return "jdbc:sqlite:" + getDbFilename();
  }

  public String getDbFilename() {
    return "pending_urls.db";
  }

  public Connection getConnection() {
    return SqliteUtil.getConnection(getDbFullname());
  }

  //  protected Transaction beginTransaction() {
  //    // return resumable ? env.beginTransaction(null, null) : null;
  //    return resumable ? getConnection().setAutoCommit(false);
  //  }

  protected void commit(Transaction tnx) throws SQLException {
    //    if (tnx != null) {
    //      tnx.commit();
    //    }
    if (!resumable) getConnection().commit();
  }

  //  protected Cursor openCursor(Transaction txn) {
  //    return urlsDB.openCursor(txn, null);
  //  }

  /**
   * 批量取max个URL
   * 
   * @param max
   * @return
   * @throws SQLException 
   */
  public List<WebURL> get(int max) throws SQLException {
    synchronized (mutex) {
      List<WebURL> results = new ArrayList<>(max);
      //      DatabaseEntry key = new DatabaseEntry();
      //      DatabaseEntry value = new DatabaseEntry();
      //      //      Transaction txn = beginTransaction();
      //      try (Cursor cursor = openCursor(txn)) {
      //        OperationStatus result = cursor.getFirst(key, value, null);
      //        int matches = 0;
      //        while ((matches < max) && (result == OperationStatus.SUCCESS)) {
      //          if (value.getData().length > 0) {
      //            results.add(webURLBinding.entryToObject(value));
      //            matches++;
      //          }
      //          result = cursor.getNext(key, value, null);
      //        }
      //      }
      //      commit(txn);
      //      return results;

      // 1. 查出最大深度
      Statement stmt = getConnection().createStatement();
      ResultSet rs = stmt.executeQuery("SELECT MAX(DEPTH) FROM " + getTablename());
      short maxDepth = -1;
      while (rs.next()) {
        maxDepth = rs.getShort(1);
        break;
      }
      rs.close();
      // 2. 取出最大深度的50条记录
      rs = stmt
        .executeQuery("SELECT ID,URL,PARENT_ID,PARENT_URL,DEPTH,ANCHOR,STATE,GMT_MODIFIED FROM "
                      + getTablename() + " WHERE DEPTH=" + maxDepth + " LIMIT 50");
      List<WebURL> urlList = new ArrayList<>();
      while (rs.next()) {
        // TODO 字段填满
        urlList.add(WebURL.fromDocRecord(new DocRecord().fillId(rs.getInt(DocsTable.C1_ID))
          .fillUrl(rs.getString(DocsTable.C2_URL)).fillParentId(rs.getInt(DocsTable.C3_PARENT_ID))
          .fillParentUrl(rs.getString(DocsTable.C4_PARENT_URL))
          .fillDepth(rs.getShort(DocsTable.C5_DEPTH)).fillAnchor(rs.getString(DocsTable.C6_ANCHOR))
          .fillGmtModified(rs.getLong(DocsTable.C8_GMTMODIFIED))));
      }
      // Assert.assertTrue(urlList.size() > 0);

      return urlList;
    }
  }

  public void delete(List<WebURL> list) throws SQLException {
    synchronized (mutex) {
      //      DatabaseEntry key = new DatabaseEntry();
      //      DatabaseEntry value = new DatabaseEntry();
      //      Transaction txn = beginTransaction();
      //      try (Cursor cursor = openCursor(txn)) {
      //        OperationStatus result = cursor.getFirst(key, value, null);
      //        int matches = 0;
      //        while ((matches < count) && (result == OperationStatus.SUCCESS)) {
      //          cursor.delete();
      //          matches++;
      //          result = cursor.getNext(key, value, null);
      //        }
      //      }
      //      commit(txn);

      Assert.assertTrue(CollectionUtils.isNotEmpty(list));
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < list.size(); i++) {
        WebURL o = list.get(i);
        sb.append(o.getDocid());
        if (i < list.size() - 1) sb.append(",");
      }

      Statement stmt = getConnection().createStatement();
      stmt.executeUpdate("DELETE FROM " + getTablename() + " WHERE ID IN (" + sb.toString() + ")");
      stmt.close();
      if (!resumable) getConnection().commit();
    }
  }

  /*
   * The key that is used for storing URLs determines the order they are
   * crawled. Lower key values results in earlier crawling. Here our keys are 6
   * bytes. The first byte comes from the URL priority. The second byte comes
   * from depth of crawl at which this URL is first found. The rest of the 4
   * bytes come from the docid of the URL. As a result, URLs with lower priority
   * numbers will be crawled earlier. If priority numbers are the same, those
   * found at lower depths will be crawled earlier. If depth is also equal,
   * those found earlier (therefore, smaller docid) will be crawled earlier.
   */
  //  protected static DatabaseEntry getDatabaseEntryKey(WebURL url) {
  //    byte[] keyData = new byte[6];
  //    keyData[0] = url.getPriority();
  //    keyData[1] = ((url.getDepth() > Byte.MAX_VALUE) ? Byte.MAX_VALUE : (byte) url.getDepth());
  //    Util.putIntInByteArray(url.getDocid(), keyData, 2);
  //    return new DatabaseEntry(keyData);
  //  }

  public void put(WebURL url) throws SQLException {
    //    DatabaseEntry value = new DatabaseEntry();
    //    webURLBinding.objectToEntry(url, value);
    //    Transaction txn = beginTransaction();
    //    urlsDB.put(txn, getDatabaseEntryKey(url), value);
    //    commit(txn);

    Assert.assertTrue(url != null);
    DocRecord doc = DocRecord.fromWebUrl(url);
    doc.assertValid();

    DocsTable.insertDoc(getConnection(), doc, getTablename());

    if (!resumable) getConnection().commit();
  }

  public int getLength() throws SQLException {
    //    return urlsDB.count();
    return SqliteUtil.queryCount(SqliteUtil.getConnection(getDbFullname()), getTablename());
  }

  public void close() throws SQLException {
    getConnection().close();
  }
}