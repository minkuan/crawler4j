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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar
 */

public class Frontier extends Configurable {
  protected static final Logger logger                           = LoggerFactory
                                                                   .getLogger(Frontier.class);

  private static final String   DATABASE_NAME                    = "PendingURLsDB";
  private static final int      IN_PROCESS_RESCHEDULE_BATCH_SIZE = 100;

  /** 待抓取URL的队列 */
  protected WorkQueues          workQueues;

  protected InProcessPagesDB    inProcessPages;

  protected final Object        mutex                            = new Object();
  protected final Object        waitingList                      = new Object();

  protected boolean             isFinished                       = false;

  protected long                scheduledPages;

  protected Counters            counters;

  public Frontier(Environment env, CrawlConfig config) {
    super(config);
    this.counters = new Counters(env, config);
    try {
      workQueues = new WorkQueues(/* env, DATABASE_NAME, */config.isResumableCrawling());
      if (config.isResumableCrawling()) {
        scheduledPages = counters.getValue(Counters.ReservedCounterNames.SCHEDULED_PAGES);
        inProcessPages = new InProcessPagesDB();
        long numPreviouslyInProcessPages = inProcessPages.getLength();
        if (numPreviouslyInProcessPages > 0) {
          logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
          scheduledPages -= numPreviouslyInProcessPages;

          List<WebURL> urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          while (!urls.isEmpty()) {
            scheduleAll(urls);
            inProcessPages.delete(urls);
            urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          }
        }
      } else {
        inProcessPages = null;
        scheduledPages = 0;
      }
    } catch (SQLException e) {
      logger.error("Error while initializing the Frontier", e);
      workQueues = null;
    } catch (ClassNotFoundException e) {
      logger.error("", e);
    }
  }

  /**
   * 将<tt>urls</tt>列表加入{@link #workQueues}。
   * 
   * @param urls
   */
  public void scheduleAll(List<WebURL> urls) {
    if (CollectionUtils.isEmpty(urls)) return;
    int maxPagesToFetch = config.getMaxPagesToFetch();
    synchronized (mutex) {
      int newScheduledPage = 0;
      for (WebURL url : urls) {
        if ((maxPagesToFetch > 0) && ((scheduledPages + newScheduledPage) >= maxPagesToFetch)) {
          break;
        }

        try {
          workQueues.put(url);
          newScheduledPage++;
        } catch (DatabaseException e) {
          logger.error("Error while putting the url in the work queue", e);
        } catch (SQLException e) {
          logger.error("", e);
        }
      }
      if (newScheduledPage > 0) {
        scheduledPages += newScheduledPage;
        // 已调度抓取计数
        counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES, newScheduledPage);
      }
      synchronized (waitingList) {
        waitingList.notifyAll();
      }
    }
  }

  //  public void schedule(WebURL url) {
  //    int maxPagesToFetch = config.getMaxPagesToFetch();
  //    synchronized (mutex) {
  //      try {
  //        if (maxPagesToFetch < 0 || scheduledPages < maxPagesToFetch) {
  //          workQueues.put(url);
  //          scheduledPages++;
  //          counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES);
  //        }
  //      } catch (DatabaseException e) {
  //        logger.error("Error while putting the url in the work queue", e);
  //      } catch (SQLException e) {
  //        logger.error("", e);
  //      }
  //    }
  //  }

  public void getNextURLs(int max, List<WebURL> result) {
    while (true) {
      synchronized (mutex) {
        if (isFinished) {
          return;
        }
        try {
          // 取出50个待抓取URL进行抓取，并将它们从待抓取队列删除。
          List<WebURL> curResults = workQueues.get(max);
          if (CollectionUtils.isNotEmpty(curResults)) workQueues.delete(curResults);
          if (inProcessPages != null) {
            for (WebURL curPage : curResults) {
              inProcessPages.put(curPage);
            }
          }
          result.addAll(curResults);
        } catch (DatabaseException e) {
          logger.error("Error while getting next urls", e);
        } catch (SQLException e) {
          logger.error("", e);
        }

        if (result.size() > 0) {
          return;
        }
      }

      try {
        synchronized (waitingList) {
          waitingList.wait();
        }
      } catch (InterruptedException ignored) {
        // Do nothing
      }
      if (isFinished) {
        return;
      }
    }
  }

  public void setProcessed(WebURL webURL) {
    counters.increment(Counters.ReservedCounterNames.PROCESSED_PAGES);
    if (inProcessPages != null) {
      if (!inProcessPages.removeURL(webURL)) {
        logger.warn("Could not remove: {} from list of processed pages.", webURL.getURL());
      }
    }
  }

  public long getQueueLength() throws SQLException {
    return workQueues.getLength();
  }

  public int getNumberOfAssignedPages() throws SQLException {
    return inProcessPages.getLength();
  }

  public long getNumberOfProcessedPages() {
    return counters.getValue(Counters.ReservedCounterNames.PROCESSED_PAGES);
  }

  public boolean isFinished() {
    return isFinished;
  }

  public void close() throws SQLException {
    workQueues.close();
    counters.close();
    if (inProcessPages != null) {
      inProcessPages.close();
    }
  }

  public void finish() {
    isFinished = true;
    synchronized (waitingList) {
      waitingList.notifyAll();
    }
  }
}