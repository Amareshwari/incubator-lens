package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;

public class QuerySuccess extends QueryEnded {
  public QuerySuccess(QueryStatus.Status prev, QueryStatus.Status current, QueryHandle handle) {
    super(prev, current, handle, null, null);
    checkCurrentState(QueryStatus.Status.SUCCESSFUL);
  }
}
