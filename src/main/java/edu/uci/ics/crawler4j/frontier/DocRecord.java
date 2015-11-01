/**
 * Alipay.com Inc. Copyright (c) 2004-2015 All Rights Reserved.
 */
package edu.uci.ics.crawler4j.frontier;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
   * 
   *
   * @author minkuan
   * @version $Id: DocsTable.java, v 0.1 Nov 1, 2015 6:28:13 PM minkuan Exp $
   */
public class DocRecord {

  /**  */
  private int      id        = -1;

  /**  */
  private String   url       = null;

  /**  */
  private int      parentId  = -1;

  /**  */
  private String   parentUrl = null;

  /**  */
  private int      depth     = -1;

  /**  */
  private String   anchor    = null;

  /**  */
  private DocState state     = DocState.INIT;

  /**  */
  private long     gmtModified;

  /**
   * Getter method for property <tt>id</tt>.
   * 
   * @return property value of id
   */
  public int getId() {
    return id;
  }

  /**
   * Setter method for property <tt>id</tt>.
   * 
   * @param id value to be assigned to property id
   * @return doc record
   */
  public DocRecord fillId(int id) {
    this.id = id;
    return this;
  }

  /**
   * Getter method for property <tt>url</tt>.
   * 
   * @return property value of url
   */
  public String getUrl() {
    return url;
  }

  /**
   * Setter method for property <tt>url</tt>.
   * 
   * @param url value to be assigned to property url
   * @return doc record
   */
  public DocRecord fillUrl(String url) {
    this.url = url;
    return this;
  }

  /**
   * Getter method for property <tt>parentId</tt>.
   * 
   * @return property value of parentId
   */
  public int getParentId() {
    return parentId;
  }

  /**
   * Setter method for property <tt>parentId</tt>.
   * 
   * @param parentId value to be assigned to property parentId
   * @return doc record
   */
  public DocRecord fillParentId(int parentId) {
    this.parentId = parentId;
    return this;
  }

  /**
   * Getter method for property <tt>parentUrl</tt>.
   * 
   * @return property value of parentUrl
   */
  public String getParentUrl() {
    return parentUrl;
  }

  /**
   * Setter method for property <tt>parentUrl</tt>.
   * 
   * @param parentUrl value to be assigned to property parentUrl
   * @return doc record
   */
  public DocRecord fillParentUrl(String parentUrl) {
    this.parentUrl = parentUrl;
    return this;
  }

  /**
   * Getter method for property <tt>depth</tt>.
   * 
   * @return property value of depth
   */
  public int getDepth() {
    return depth;
  }

  /**
   * Setter method for property <tt>depth</tt>.
   * 
   * @param depth value to be assigned to property depth
   * @return doc record
   */
  public DocRecord fillDepth(int depth) {
    this.depth = depth;
    return this;
  }

  /**
   * Getter method for property <tt>gmtModified</tt>.
   * 
   * @return property value of gmtModified
   */
  public long getGmtModified() {
    return gmtModified;
  }

  /**
   * Setter method for property <tt>gmtModified</tt>.
   * 
   * @param gmtModified value to be assigned to property gmtModified
   * @return doc record
   */
  public DocRecord fillGmtModified(long gmtModified) {
    this.gmtModified = gmtModified;
    return this;
  }

  /**
   * Getter method for property <tt>state</tt>.
   * 
   * @return property value of state
   */
  public int getState() {
    return state.getCode();
  }

  /**
   * Setter method for property <tt>state</tt>.
   * 
   * @param state value to be assigned to property state
   * @return doc record
   */
  public DocRecord fillState(DocState state) {
    this.state = state;
    return this;
  }

  /**
   * Getter method for property <tt>anchor</tt>.
   * 
   * @return property value of anchor
   */
  public String getAnchor() {
    return anchor;
  }

  /**
   * Setter method for property <tt>anchor</tt>.
   * 
   * @param anchor value to be assigned to property anchor
   */
  public DocRecord fillAnchor(String anchor) {
    this.anchor = anchor;
    return this;
  }

  /** 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

}
