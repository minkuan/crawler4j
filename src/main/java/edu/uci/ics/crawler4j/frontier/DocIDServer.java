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
import static edu.uci.ics.crawler4j.frontier.DocsTable.insertDoc;
import static edu.uci.ics.crawler4j.tests.sqlite.SqliteUtil.ensureTableExists;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.tests.sqlite.SqliteUtil;

/**
 * 
 * docId库：1. 存储所有发现的URL；2. 判断一个URL是否已经发现过。
 * 
 * @author Yasser Ganjisaffar
 */
@SuppressWarnings("javadoc")
public class DocIDServer extends Configurable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocIDServer.class);

  // private final Database      docIDsDB;
  // private static final String DATABASE_NAME = "DocIDs";

  private final Object        mutex  = new Object();

  private int                 lastDocID;

  public DocIDServer(Environment env, CrawlConfig config) throws ClassNotFoundException,
                                                         SQLException {
    super(config);
    //    DatabaseConfig dbConfig = new DatabaseConfig();
    //    dbConfig.setAllowCreate(true);
    //    dbConfig.setTransactional(config.isResumableCrawling());
    //    dbConfig.setDeferredWrite(!config.isResumableCrawling());
    lastDocID = 0;
    //    docIDsDB = env.openDatabase(null, DATABASE_NAME, dbConfig);
    //    if (config.isResumableCrawling()) {
    //      int docCount = getDocCount();
    //      if (docCount > 0) {
    //        logger.info("Loaded {} URLs that had been detected in previous crawl.", docCount);
    //        lastDocID = docCount;
    //      }
    //    }

    if (!config.isResumableCrawling()) {
      SqliteUtil.deleteDbFileIfExists(getDbFilename());
    }
    SqliteUtil.ensureDbConn(getDbFullname(), config.isResumableCrawling());
    SqliteUtil.ensureDbFileExists(getDbFilename());
    //    "CREATE TABLE IF NOT EXISTS %s " + "(ID INT PRIMARY KEY NOT NULL,"
    //    + " NAME TEXT NOT NULL, " + " AGE INT NOT NULL, "
    //    + " ADDRESS CHAR(50), " + " SALARY REAL)", TABLE_NAME
    ensureTableExists(getConnection(), String.format(CREATE_CLAUSE, getTablename()));

    lastDocID = SqliteUtil.queryCount(getConnection(), getTablename());
    if (lastDocID > 0)
      if (LOGGER.isInfoEnabled())
        LOGGER.info("Loaded {} URLs that had been detected in previous crawl.", lastDocID);

  }

  /**
   * Returns the docid of an already seen url.
   *
   * @param url the URL for which the docid is returned.
   * @return the docid of the url if it is seen before. Otherwise -1 is returned.
   */
  public DocRecord getDocRecord(String url) {
    synchronized (mutex) {
      //      OperationStatus result = null;
      //      DatabaseEntry value = new DatabaseEntry();
      try {
        //        DatabaseEntry key = new DatabaseEntry(url.getBytes());
        //        result = docIDsDB.get(null, key, value, null);

        DocRecord doc = DocsTable.queryByUrl(url, getConnection(), getTablename());
        return doc; // possible to be null

      } catch (Exception e) {
        LOGGER.error("Exception thrown while getting DocID", e);
        // return -1;
      }

      //      if ((result == OperationStatus.SUCCESS) && (value.getData().length > 0)) {
      //        return Util.byteArray2Int(value.getData());
      //      }

      return null;
    }
  }

  public DocRecord genNewDocRecordIfNotExists(final DocRecord inputDoc) {
    Assert.assertTrue(inputDoc.toString(),
      inputDoc != null && StringUtils.isNotBlank(inputDoc.getUrl()));

    synchronized (mutex) {
      try {
        // Make sure that we have not already assigned a docid for this URL
        DocRecord doc = getDocRecord(inputDoc.getUrl());
        // if (doc == null) return null;

        if (doc == null || doc.getId() < 0) {
          ++lastDocID;
          // TODO 填属性
          insertDoc(getConnection(), inputDoc.fillId(lastDocID), getTablename());
          doc = inputDoc;
        }
        return doc;
      } catch (Exception e) {
        LOGGER.error("Exception thrown while generating new DocID", e);
        return null;
      }
    }
  }

  public void addUrlAndDocId(final DocRecord doc) throws Exception {
    synchronized (mutex) {
      if (doc.getId() <= lastDocID) {
        throw new Exception("Requested doc id: " + doc.getId() + " is not larger than: "
                            + lastDocID);
      }

      // Make sure that we have not already assigned a docid for this URL
      // 1. 判断是否已经存在
      DocRecord prevDoc = getDocRecord(doc.getUrl());
      if (prevDoc == null || prevDoc.getId() > 0) {
        // TODO 已经存在
        if (prevDoc.getId() == doc.getId()) {
          return;
        }
        throw new Exception("Doc id: " + prevDoc + " is already assigned to URL: " + doc.getUrl());
      }

      //      docIDsDB.put(null, new DatabaseEntry(url.getBytes()),
      //        new DatabaseEntry(Util.int2ByteArray(docId)));
      // 2. 若不存在，则插入docId库。
      DocsTable.insertDoc(getConnection(), doc, getTablename());

      lastDocID = doc.getId();
    }
  }

  public String getTablename() {
    return "docs";
  }

  public String getDbFullname() {
    return "jdbc:sqlite:" + getDbFilename();
  }

  public String getDbFilename() {
    return "docid.db";
  }

  public Connection getConnection() {
    return SqliteUtil.getConnection(getDbFullname());
  }

  public boolean isSeenBefore(String url) {
    DocRecord doc = getDocRecord(url);
    if (doc != null) {
      return doc.getId() != -1;
    }
    return false;
  }

  public final int getDocCount() {
    try {
      return SqliteUtil.queryCount(getConnection(), getTablename());
      //      return (int) docIDsDB.count();
    } catch (SQLException e) {
      LOGGER.error("Exception thrown while getting DOC Count", e);
      return -1;
    }
  }

  public void close() {
    try {
      // docIDsDB.close();
      getConnection().close();
    } catch (SQLException e) {
      LOGGER.error("Exception thrown while closing DocIDServer", e);
    }
  }
}