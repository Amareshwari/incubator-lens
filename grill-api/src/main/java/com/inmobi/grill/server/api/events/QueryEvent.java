package com.inmobi.grill.server.api.events;


import com.inmobi.grill.api.QueryHandle;

import java.util.UUID;

/**
 * A generic event related to state change of a query
 * Subclasses must declare the specific type of change they are interested in.
 *
 * Every event will have an ID, which should be used by listeners to check if the event is already received.
 *
 * @param <T> Type of changed information about the query
 */
public abstract class QueryEvent<T> {
  protected final T previousValue;
  protected final T currentValue;
  protected final QueryHandle handle;
  protected final UUID id = UUID.randomUUID();

  public QueryEvent(T prev, T current, QueryHandle handle) {
    previousValue = prev;
    currentValue = current;
    this.handle = handle;
  }

  public final T getPreviousValue() {
    return previousValue;
  }

  public final T getCurrentValue() {
    return currentValue;
  }

  public final QueryHandle getQueryHandle() {
    return handle;
  }

  public final UUID getId() {
    return id;
  }
}
