package com.inmobi.grill.server.api.events.query;

import com.inmobi.grill.query.QueryHandle;

/**
 * Event fired when query moves up or down in the execution engine's queue
 */
public class QueuePositionChange extends QueryEvent<Integer> {
  public QueuePositionChange(long eventTime, Integer prev, Integer current, QueryHandle handle) {
    super(eventTime, prev, current, handle);
  }
}
