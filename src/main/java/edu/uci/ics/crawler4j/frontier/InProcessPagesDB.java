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

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.url.WebURL;

/**
 * This class maintains the list of pages which are
 * assigned to crawlers but are not yet processed.
 * It is used for resuming a previous crawl.
 *
 * @author Yasser Ganjisaffar
 */
public class InProcessPagesDB extends WorkQueues {
  private static final Logger logger = LoggerFactory.getLogger(InProcessPagesDB.class);

  //  private static final String DATABASE_NAME       = "InProcessPagesDB";

  public InProcessPagesDB(/* Environment env */) throws SQLException, ClassNotFoundException {
    super(/* env, DATABASE_NAME, */true);
    int docCount = getLength();
    if (docCount > 0) {
      logger.info("Loaded {} URLs that have been in process in the previous crawl.", docCount);
    }

    //    if (!resumable) {
    //      SqliteUtil.deleteDbFileIfExists(getDbFilename());
    //    }
    //    SqliteUtil.ensureDbConn(PROCESS_DB_FULLNAME, false);
    //    SqliteUtil.ensureDbFileExists(PROCESS_DB_NAME);
    //    "CREATE TABLE IF NOT EXISTS %s " + "(ID INT PRIMARY KEY NOT NULL,"
    //    + " NAME TEXT NOT NULL, " + " AGE INT NOT NULL, "
    //    + " ADDRESS CHAR(50), " + " SALARY REAL)", TABLE_NAME
    //    ensureTableExists(getConnection(), CREATE_CLAUSE);

    //    /* lastDocID = */SqliteUtil.queryCount(SqliteUtil.getConnection(PROCESS_DB_FULLNAME),
    //      PROCESS_TABLE);
    //    if (lastDocID > 0)
    //      if (LOGGER.isInfoEnabled())
    //        LOGGER.info("Loaded {} URLs that had been detected in previous crawl.", lastDocID);

  }

  //  private Connection getConnection() {
  //    return SqliteUtil.getConnection(getDbFullname());
  //  }

  //  public int getLength() throws SQLException {
  //    //    return urlsDB.count();
  //    return SqliteUtil.queryCount(getConnection(), getTablename());
  //  }

  public boolean removeURL(WebURL webUrl) {
    Assert.assertNotNull(webUrl);
    Assert.assertTrue(StringUtils.isNotBlank(webUrl.getURL()));
    synchronized (mutex) {
      //      DatabaseEntry key = getDatabaseEntryKey(webUrl);
      //      DatabaseEntry value = new DatabaseEntry();
      //      Transaction txn = beginTransaction();
      //      try (Cursor cursor = openCursor(txn)) {
      //        OperationStatus result = cursor.getSearchKey(key, value, null);
      //
      //        if (result == OperationStatus.SUCCESS) {
      //          result = cursor.delete();
      //          if (result == OperationStatus.SUCCESS) {
      //            return true;
      //          }
      //        }
      //      } finally {
      //        commit(txn);
      //      }

      Statement stmt;
      try {
        stmt = getConnection().createStatement();
        stmt
          .executeUpdate("DELETE FROM " + getTablename() + " WHERE URL='" + webUrl.getURL() + "'");
        if (!resumable) getConnection().commit();
        return true;
      } catch (SQLException e) {
        logger.error("", e);
      }

      return false;
    }
  }

  /** 
   * @see edu.uci.ics.crawler4j.frontier.WorkQueues#getTablename()
   */
  @Override
  public String getTablename() {
    return "process";
  }

  /** 
   * @see edu.uci.ics.crawler4j.frontier.WorkQueues#getDbFullname()
   */
  @Override
  public String getDbFullname() {
    return "jdbc:sqlite:" + getDbFilename();
  }

  /** 
   * @see edu.uci.ics.crawler4j.frontier.WorkQueues#getDbFilename()
   */
  @Override
  public String getDbFilename() {
    return "process.db";
  }
}