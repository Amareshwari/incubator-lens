package com.inmobi.grill.server.api.query;

import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryStatus;

/**
 * Event fired when a query is closed.
 */
public class QueryClosed extends QueryEnded {
  public QueryClosed(long eventTime, QueryStatus.Status prev,
      QueryStatus.Status current, QueryHandle handle, String user, String cause) {
    super(eventTime, prev, current, handle, user, cause);
    checkCurrentState(QueryStatus.Status.CLOSED);
  }
}
