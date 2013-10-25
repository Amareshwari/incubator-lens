package com.inmobi.grill.api;

import java.util.UUID;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class QueryHandle extends QuerySubmitResult {
  @XmlElement
  private UUID handleId;

  public QueryHandle() {
    
  }
  public QueryHandle(UUID handleId) {
    this.handleId = handleId;
  }

  public UUID getHandleId() {
    return handleId;
  }

  public static QueryHandle fromString(String handle) {
    return new QueryHandle(UUID.fromString(handle));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((handleId == null) ? 0 : handleId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof QueryHandle)) {
      return false;
    }
    QueryHandle other = (QueryHandle) obj;
    if (handleId == null) {
      if (other.handleId != null) {
        return false;
      }
    } else if (!handleId.equals(other.handleId)) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
  	return handleId.toString();
  }
}
