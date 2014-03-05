package com.inmobi.grill.driver.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.TaskStatus;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TStringValue;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.inmobi.grill.api.GrillConfConstatnts;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

public class HiveDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(HiveDriver.class);

  public static final String GRILL_PERSISTENT_RESULT_SET = "grill.persistent.resultset";
  public static final String GRILL_RESULT_SET_PARENT_DIR = "grill.result.parent.dir";
  public static final String GRILL_HIVE_CONNECTION_CLASS = "grill.hive.connection.class";
  public static final String GRILL_RESULT_SET_PARENT_DIR_DEFAULT = "/tmp/grillreports";
  public static final String GRILL_ADD_INSERT_OVEWRITE = "grill.add.insert.overwrite";
  public static final String GRILL_OUTPUT_DIRECTORY_FORMAT = "grill.result.output.dir.format";
  public static final String GRILL_CONNECTION_EXPIRY_DELAY = "grill.hs2.connection.expiry.delay";
  // Default expiry is 10 minutes
  public static final long DEFAULT_EXPIRY_DELAY = 600 * 1000;

  private HiveConf conf;
  private SessionHandle session;
  private Map<QueryHandle, QueryContext> handleToContext;
  private final Lock sessionLock;

  private final Map<Long, ExpirableConnection> threadConnections = 
      new HashMap<Long, ExpirableConnection>();
  private final DelayQueue<ExpirableConnection> thriftConnExpiryQueue = 
      new DelayQueue<ExpirableConnection>();
  private final Thread connectionExpiryThread = new Thread(new ConnectionExpiryRunnable());

  class ConnectionExpiryRunnable implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          ExpirableConnection expired = thriftConnExpiryQueue.take();
          expired.setExpired();
          ThriftConnection thConn = expired.getConnection();

          if (thConn != null) {
            try {
              LOG.info("Closed connection:" + expired.getConnId());
              thConn.close();
            } catch (IOException e) {
              LOG.error("Error closing connection", e);
            }
          }
        }
      } catch (InterruptedException intr) {
        LOG.warn("Connection expiry thread interrupted", intr);
        return;
      }
    }
  }

  private static final AtomicInteger connectionCounter = new AtomicInteger();
  static class ExpirableConnection implements Delayed {
    long accessTime;
    private final ThriftConnection conn;
    private final long timeout;
    private volatile boolean expired;
    private final int connId;

    public ExpirableConnection(ThriftConnection conn, HiveConf conf) {
      this.conn = conn;
      this.timeout = 
          conf.getLong(GRILL_CONNECTION_EXPIRY_DELAY, DEFAULT_EXPIRY_DELAY);
      connId = connectionCounter.incrementAndGet();
      accessTime = System.currentTimeMillis();
    }

    private ThriftConnection getConnection() {
      accessTime = System.currentTimeMillis();
      return conn;
    }

    private boolean isExpired() {
      return expired;
    }

    private void setExpired() {
      expired = true;
    }

    private int getConnId() {
      return connId;
    }

    @Override
    public int compareTo(Delayed other) {
      return (int)(this.getDelay(TimeUnit.MILLISECONDS)
          - other.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long age = System.currentTimeMillis() - accessTime;
      return unit.convert(timeout - age, TimeUnit.MILLISECONDS) ;
    }
  }

  int openConnections() {
    return thriftConnExpiryQueue.size();
  }

  /**
   * Internal class to hold query related info
   */
  public static class QueryContext implements Serializable {
    private static final long serialVersionUID = -85736670291184351L;
    transient QueryHandle queryHandle;
    transient OperationHandle hiveHandle;
    String userQuery;
    String hiveQuery;
    transient Path resultSetPath;
    boolean isPersistent;
    transient HiveConf conf;

    public QueryContext(QueryHandle handle) {
      this.queryHandle = handle;
    }
    
    public QueryContext() {
      queryHandle = new QueryHandle(UUID.randomUUID());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof QueryContext) {
        return queryHandle.equals(((QueryContext) obj).queryHandle);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return queryHandle.hashCode();
    }

    @Override
    public String toString() {
      return queryHandle + "/" + userQuery;
    }
  }

  public HiveDriver() throws GrillException {
    this.sessionLock = new ReentrantLock();
    this.handleToContext = new HashMap<QueryHandle, QueryContext>();
    connectionExpiryThread.setDaemon(true);
    connectionExpiryThread.setName("HiveDriver-ConnectionExpiryThread");
    connectionExpiryThread.start();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    this.conf = new HiveConf(conf, HiveDriver.class);
  }

  @Override
  public QueryPlan explain(String query, Configuration conf)
      throws GrillException {
    QueryContext ctx = createQueryContext(query, conf);
    // Get result set of explain
    Configuration explainConf = new Configuration(conf);
    explainConf.setBoolean(GRILL_PERSISTENT_RESULT_SET, false);
    String explainQuery = "EXPLAIN EXTENDED " + ctx.hiveQuery;
    HiveInMemoryResultSet inMemoryResultSet = (HiveInMemoryResultSet) execute(
        explainQuery, explainConf);
    List<String> explainOutput = new ArrayList<String>();
    while (inMemoryResultSet.hasNext()) {
      explainOutput.add(((TStringValue) inMemoryResultSet.next().get(0)).getValue());
    }

    QueryHandle handle = null;
    if (conf.getBoolean(GrillConfConstatnts.PREPARE_ON_EXPLAIN,
        GrillConfConstatnts.DEFAULT_PREPARE_ON_EXPLAIN)) {
      handleToContext.put(ctx.queryHandle, ctx);
      handle = ctx.queryHandle;
    }
    LOG.info("Explain: " + query);
    try {
      return new HiveQueryPlan(explainOutput, handle, ctx.conf);
    } catch (HiveException e) {
      throw new GrillException("Unable to create hive query plan", e);
    }
  }

  @Override
  public GrillResultSet execute(String query, Configuration conf) throws GrillException {
    // Get eventual Hive query based on conf
    QueryContext ctx = createQueryContext(query, conf);
    LOG.info("Execute: " + query);
    return execute(ctx);
  }

  private GrillResultSet execute(QueryContext ctx) throws GrillException {
    try {
      ctx.conf.set("mapred.job.name", ctx.queryHandle.toString());
      OperationHandle op = getClient().executeStatement(getSession(), ctx.hiveQuery, 
          ctx.conf.getValByRegex(".*"));
      ctx.hiveHandle = op;
      LOG.info("The hive operation handle: " + ctx.hiveHandle);
      OperationStatus status = getClient().getOperationStatus(op);

      if (status.getState() == OperationState.ERROR) {
        throw new GrillException("Unknown error while running query " + ctx.userQuery);
      }
      return createResultSet(ctx);
    } catch (HiveSQLException hiveErr) {
      throw new GrillException("Error executing query" , hiveErr);
    }

  }
  @Override
  public QueryHandle executeAsync(String query, Configuration conf) throws GrillException {
    LOG.info("ExecuteAsync: " + query);
    QueryContext ctx = createQueryContext(query, conf);
    handleToContext.put(ctx.queryHandle, ctx);
    return executeAsync(ctx);
  }

  private QueryHandle executeAsync(QueryContext ctx)
      throws GrillException {
    try {
      ctx.conf.set("mapred.job.name", ctx.queryHandle.toString());
      ctx.hiveHandle = getClient().executeStatementAsync(getSession(), ctx.hiveQuery, 
          ctx.conf.getValByRegex(".*"));
      LOG.info("QueryHandle: " + ctx.queryHandle.getHandleId() + " HiveHandle:" +
          ctx.hiveHandle);
    } catch (HiveSQLException e) {
      throw new GrillException("Error executing async query", e);
    }
    return ctx.queryHandle;
  }

  private void copyConf(QueryContext ctx, Configuration conf) {
    for (Map.Entry<String, String> entry : conf) {
      ctx.conf.set(entry.getKey(), entry.getValue());
    }
  }
  @Override
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws GrillException {
    LOG.info("ExecutePrepared: " + handle);
    QueryContext ctx = getContext(handle);
    copyConf(ctx, conf);
    return execute(ctx);
  }

  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryContext ctx = getContext(handle);
    copyConf(ctx, conf);
    executeAsync(ctx);
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle)  throws GrillException {
    LOG.debug("GetStatus: " + handle);
    QueryContext ctx = getContext(handle);
    ByteArrayInputStream in = null;
    try {
      // Get operation status from hive server
      LOG.debug("GetStatus hiveHandle: " + ctx.hiveHandle);
      OperationStatus opStatus = getClient().getOperationStatus(ctx.hiveHandle);
      LOG.debug("GetStatus on hiveHandle: " + ctx.hiveHandle + " returned state:" + opStatus.getState());
      QueryStatus.Status stat = null;
      String statusMessage;

      switch (opStatus.getState()) {
      case CANCELED:
        stat = Status.CANCELED;
        statusMessage = "Query has been cancelled!";
        break;
      case CLOSED:
        stat = Status.CLOSED;
        statusMessage = "Query has been closed!";
        break;
      case ERROR:
        stat = Status.FAILED;
        statusMessage = "Query failed with errorCode:" +
            opStatus.getOperationException().getErrorCode() +
            " with errorMessage: " + opStatus.getOperationException().getMessage();
        break;
      case FINISHED:
        statusMessage = "Query is successful!"; 
        stat = Status.SUCCESSFUL;
        break;
      case INITIALIZED:
        statusMessage = "Query is initiazed in HiveServer!";
        stat = Status.RUNNING;
        break;
      case RUNNING:
        statusMessage = "Query is running in HiveServer!";
        stat = Status.RUNNING;
        break;
      case PENDING:
        statusMessage = "Query is pending in HiveServer";
        stat = Status.PENDING;
        break;
      case UNKNOWN:
        statusMessage = "Query is in unknown state at HiveServer ";
        stat = Status.UNKNOWN;
        break;
      default :
        statusMessage = "";
        break;
      }

      float progress = 0f;
      String jsonTaskStatus = opStatus.getTaskStatus();
      String errorMsg = null;
      if (StringUtils.isNotBlank(jsonTaskStatus)) {
        ObjectMapper mapper = new ObjectMapper();
        in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
        List<TaskStatus> taskStatuses = 
            mapper.readValue(in, new TypeReference<List<TaskStatus>>() {});
        int completedTasks = 0;
        StringBuilder errorMessage = new StringBuilder();
        for (TaskStatus taskStat : taskStatuses) {
          String state = taskStat.getTaskState();
          if ("FINISHED_STATE".equalsIgnoreCase(state)) {
            completedTasks++;
          }
          if ("FAILED_STATE".equalsIgnoreCase(state)) {
            appendTaskIds(errorMessage, taskStat);
            errorMessage.append(" has failed! ");
          }
        }
        progress = taskStatuses.size() == 0 ? 0 : (float)completedTasks/taskStatuses.size();
        errorMsg = errorMessage.toString();
      } else {
        LOG.warn("Empty task statuses");
      }
      QueryStatus status = new QueryStatus(progress, stat, statusMessage, false, ctx.hiveHandle.getHandleIdentifier().toString());
      status.setProgressMessage(jsonTaskStatus);
      if (StringUtils.isNotBlank(errorMsg)) {
        status.setErrorMessage(errorMsg);
      } else if (stat.equals(Status.FAILED)) {
        status.setErrorMessage(statusMessage);
      }
      return status;
    } catch (Exception e) {
      LOG.error("Error getting query status", e);
      throw new GrillException("Error getting query status", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void appendTaskIds(StringBuilder message, TaskStatus taskStat) {
    message.append(taskStat.getTaskId()).append("(");
    message.append(taskStat.getType()).append("):");
    if (taskStat.getExternalHandle() != null) {
      message.append(taskStat.getExternalHandle()).append(":");
    }    
  }

  @Override
  public GrillResultSet fetchResultSet(QueryHandle handle)  throws GrillException {
    LOG.info("FetchResultSet: " + handle);
    // This should be applicable only for a async query
    QueryContext ctx = getContext(handle);
    return createResultSet(ctx);
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    LOG.info("CloseQuery: " + handle);
    QueryContext options = handleToContext.remove(handle);
    if (options != null) {
      LOG.info("CloseQuery: " + options.hiveHandle);
      OperationHandle opHandle = options.hiveHandle;
      if (opHandle != null) {
        try {
          getClient().closeOperation(opHandle);
        } catch (HiveSQLException e) {
          throw new GrillException("Unable to close query", e);
        }
      }
    }
  }

  @Override
  public boolean cancelQuery(QueryHandle handle)  throws GrillException {
    LOG.info("CancelQuery: " + handle);
    QueryContext ctx = getContext(handle);
    try {
      LOG.info("CancelQuery hiveHandle: " + ctx.hiveHandle);
      getClient().cancelOperation(ctx.hiveHandle);
      return true;
    } catch (HiveSQLException e) {
      throw new GrillException();
    }
  }

  @Override
  public void close() {
    LOG.info("CloseDriver");
    // Close this driver and release all resources
    for (QueryHandle query : new ArrayList<QueryHandle>(handleToContext.keySet())) {
      try {
        closeQuery(query);
      } catch (GrillException exc) {
        LOG.warn("Could not close query: " +  query, exc);
      }
    }

    try {
      getClient().closeSession(getSession());
    } catch (Exception e) {
      LOG.error("Unable to close connection", e);
    }
  }

  protected CLIServiceClient getClient() throws GrillException {
    ExpirableConnection connection = threadConnections.get(Thread.currentThread().getId());
    if (connection == null || connection.isExpired()) {
      Class<? extends ThriftConnection> clazz = conf.getClass(
          GRILL_HIVE_CONNECTION_CLASS, 
          EmbeddedThriftConnection.class, 
          ThriftConnection.class);
      try {
        ThriftConnection tconn = clazz.newInstance();
        connection = new ExpirableConnection(tconn, conf);
        thriftConnExpiryQueue.offer(connection);
        threadConnections.put(Thread.currentThread().getId(), connection);
        LOG.info("New thrift connection " + clazz.getName() + " ID=" + connection.getConnId());
      } catch (Exception e) {
        throw new GrillException(e);
      }
    } else {
      synchronized(thriftConnExpiryQueue) {
        thriftConnExpiryQueue.remove(connection);
        thriftConnExpiryQueue.offer(connection);
      }
    }

    return connection.getConnection().getClient(conf);
  }

  private GrillResultSet createResultSet(QueryContext context)
      throws GrillException {
    LOG.info("Creating result set for hiveHandle:" + context.hiveHandle);
    if (context.isPersistent) {
      return new HivePersistentResultSet(context.resultSetPath,
          context.hiveHandle, getClient(), context.queryHandle);
    } else {
      return new HiveInMemoryResultSet(context.hiveHandle, getClient());
    }
  }

  QueryContext createQueryContext(String query, Configuration conf) {
    QueryContext ctx = new QueryContext();
    ctx.conf = new HiveConf(conf, HiveDriver.class);
    ctx.isPersistent = conf.getBoolean(GRILL_PERSISTENT_RESULT_SET, true);
    ctx.userQuery = query;

    if (ctx.isPersistent && conf.getBoolean(GRILL_ADD_INSERT_OVEWRITE, true)) {
      // store persistent data into user specified location
      // If absent, take default home directory
      String resultSetParentDir = conf.get(GRILL_RESULT_SET_PARENT_DIR);
      StringBuilder builder;
      if (StringUtils.isNotBlank(resultSetParentDir)) {
        ctx.resultSetPath = new Path(resultSetParentDir, ctx.queryHandle.toString());
        // create query
        builder = new StringBuilder("INSERT OVERWRITE DIRECTORY ");
      } else {
        // Write to /tmp/grillreports
        ctx.resultSetPath = new
            Path(GRILL_RESULT_SET_PARENT_DIR_DEFAULT, ctx.queryHandle.toString());
        builder = new StringBuilder("INSERT OVERWRITE LOCAL DIRECTORY ");
      }
      builder.append('"').append(ctx.resultSetPath).append("\" ");
      String outputDirFormat = conf.get(GRILL_OUTPUT_DIRECTORY_FORMAT);
      if (outputDirFormat != null) {
        builder.append(outputDirFormat);
      }
      builder.append(' ').append(ctx.userQuery).append(' ');
      ctx.hiveQuery =  builder.toString();
    } else {
      ctx.hiveQuery = ctx.userQuery;
    }

    return ctx;
  }

  private SessionHandle getSession() throws GrillException {
    sessionLock.lock();
    try {
      if (session == null) {
        try {
          String userName = conf.getUser();
          session = getClient().openSession(userName, "");
          LOG.info("New session: " + session.getSessionId());
        } catch (Exception e) {
          throw new GrillException(e);
        }
      }
    } finally {
      sessionLock.unlock();
    }
    return session;
  }

  private QueryContext getContext(QueryHandle handle) throws GrillException {
    QueryContext ctx = handleToContext.get(handle);
    if (ctx == null) {
      throw new GrillException("Query not found " + ctx); 
    }
    return ctx;
  }
  
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int numPairs = in.readInt();
    synchronized(handleToContext) {
      for (int i = 0; i < numPairs; i++) {
        QueryHandle handle = new QueryHandle((UUID)in.readObject());
        String resultSetPath = in.readUTF();
        Path resultPath = new Path(resultSetPath);
        OperationHandle opHandle = new OperationHandle((TOperationHandle) in.readObject());
        QueryContext ctx = (QueryContext) in.readObject();
        ctx.hiveHandle = opHandle;
        ctx.resultSetPath = resultPath;
        ctx.queryHandle = handle;
        handleToContext.put(ctx.queryHandle, ctx);
      }
    }
  }


  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // Write the query handle to hive handle map to output
    synchronized(handleToContext) {
      out.writeInt(handleToContext.size());
      for (QueryContext queryContext : handleToContext.values()) {
        out.writeObject(queryContext.queryHandle.getHandleId());
        out.writeUTF(queryContext.resultSetPath.toString());
        out.writeObject(queryContext.hiveHandle.toTOperationHandle());
        out.writeObject(queryContext);
      }
    }
  }
}
