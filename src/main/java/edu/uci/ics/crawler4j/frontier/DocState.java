/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.frontier;

/**
 * 
 *
 * @author minkuan
 * @version $Id: a.java, v 0.1 Nov 1, 2015 7:36:41 PM minkuan Exp $
 */

public enum DocState {

  /**  */
  INIT(0, "INIT"),

  /**  */
  DOWNLOADING(1, "DOWNLOADING"),

  /**  */
  FETCHED(2, "DOWNLOADED");

  /** code */
  private int    code;

  /** description */
  private String desc;

  /**
   * @param code code 
   * @param desc description
   */
  DocState(int code, String desc) {
    this.code = code;
    this.desc = desc;
  }

  /**
   * Getter method for property <tt>code</tt>.
   * 
   * @return property value of code
   */
  public int getCode() {
    return code;
  }

  /**
   * Setter method for property <tt>code</tt>.
   * 
   * @param code value to be assigned to property code
   */
  public void setCode(int code) {
    this.code = code;
  }

  /**
   * Getter method for property <tt>desc</tt>.
   * 
   * @return property value of desc
   */
  public String getDesc() {
    return desc;
  }

  /**
   * Setter method for property <tt>desc</tt>.
   * 
   * @param desc value to be assigned to property desc
   */
  public void setDesc(String desc) {
    this.desc = desc;
  }
}