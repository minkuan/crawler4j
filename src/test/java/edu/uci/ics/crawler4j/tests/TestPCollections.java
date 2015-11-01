/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.tests;

import org.pcollections.HashTreePSet;
import org.pcollections.PSet;

/**
 * 
 *
 * @author minkuan
 * @version $Id: TestPCollections.java, v 0.1 Nov 1, 2015 2:02:11 AM minkuan Exp $
 */
public class TestPCollections {

  /**
   * 
   * @param args CL
   */
  public static void main(String... args) {
    PSet<String> set = HashTreePSet.empty();
    set = set.plus("something");
    System.out.println(set);
    System.out.println(set.plus("something else"));
    System.out.println(set);
  }

}
