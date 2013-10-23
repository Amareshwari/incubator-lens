package com.inmobi.grill.api;

import org.apache.hadoop.conf.Configuration;

import com.inmobi.grill.exception.GrillException;

public interface GrillDriver {

  /**
   * Get driver configuration
   * 
   */
  public Configuration getConf();

  /** 
   * Configure driver with {@link Configuration} passed
   * 
   * @param conf The configuration object
   */
  public void configure(Configuration conf) throws GrillException;

  /**
   * Explain the given query
   * 
   * @param query The query should be in HiveQL(SQL like)
   * @param conf The query configuration
   * 
   * @return The query plan object;
   * 
   * @throws GrillException
   */
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException;

  /**
   * Prepare the given query
   * 
   * @param pContext 
   * 
   * @throws GrillException
   */
  public void prepare(PreparedQueryContext pContext) throws GrillException;

  /**
   * Explain and prepare the given query
   * 
   * @param pContext 
   * 
   * @return The query plan object;
   * 
   * @throws GrillException
   */
  public QueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException;

  /**
   * Close the prepare query specified by the prepared handle,
   * releases all the resources held by the prepared query.
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closePreparedQuery(QueryPrepareHandle handle) throws GrillException;

  /**
   * Blocking execute of the query
   * 
   * @param context 
   * 
   * @return returns the result set
   * 
   * @throws GrillException
   */
  public GrillResultSet execute(QueryContext context)
      throws GrillException;

  /**
   * Asynchronously execute the query
   * 
   * @param context The query context
   * 
   * @return a query handle, which can used to know the status.
   * 
   * @throws GrillException
   */
  public void executeAsync(QueryContext context)
      throws GrillException;

  /**
   * Get status of the query, specified by the handle
   * 
   * @param handle The query handle
   * 
   * @return query status
   */
  public QueryStatus getStatus(QueryHandle handle) throws GrillException;

  /**
   * Fetch the results of the query, specified by the handle
   * 
   * @param handle The query handle
   * 
   * @return returns the result set
   */
  public GrillResultSet fetchResultSet(QueryContext context) throws GrillException;

  /**
   * Close the resultset for the query
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closeResultSet(QueryHandle handle) throws GrillException;

  /**
   * Cancel the execution of the query, specified by the handle
   * 
   * @param handle The query handle.
   * 
   * @return true if cancel was successful, false otherwise
   */
  public boolean cancelQuery(QueryHandle handle) throws GrillException;

  /**
   * Close the query specified by the handle, releases all the resources
   * held by the query.
   * 
   * @param handle The query handle
   * 
   * @throws GrillException
   */
  public void closeQuery(QueryHandle handle) throws GrillException;

  /**
   * Close the driver, releasing all resouces used up by the driver
   * @throws GrillException
   */
  public void close() throws GrillException;
}
