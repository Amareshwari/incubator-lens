package com.inmobi.grill.driver.impala;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.beeswax.api.BeeswaxException;
import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryNotFoundException;
import com.cloudera.beeswax.api.QueryState;
import com.cloudera.impala.thrift.ImpalaService;
import com.cloudera.impala.thrift.ImpalaService.Client;
import com.inmobi.grill.api.GrillDriver;
import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.PreparedQueryContext;
import com.inmobi.grill.api.QueryCompletionListener;
import com.inmobi.grill.api.QueryContext;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.exception.GrillException;

public class ImpalaDriver implements GrillDriver {

  Logger logger = Logger.getLogger(ImpalaDriver.class.getName());

  private Client client;
  private List<String> storages = new ArrayList<String>();

  public ImpalaDriver() {
  }

  @Override
  public QueryPlan explain(String query, Configuration conf) {
    /*QueryCost q = new QueryCost();
    q.setExecMode(ExecMode.INTERACTIVE);
    q.setScanMode(ScanMode.FULL_SCAN);
    q.setScanSize(-1);

    return q;*/
    return null;
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    Query q = new Query();
    q.query = query;
    QueryHandle queryHandle;
    try {
      queryHandle = client.query(q);
      QueryState qs = QueryState.INITIALIZED;

      while (true) {
        qs = client.get_state(queryHandle);
        logger.info("Query state is for query" + q + "is " + qs);
        if (qs == QueryState.FINISHED) {
          break;
        }
        if (qs == QueryState.EXCEPTION) {
          logger.error("Query aborted, unable to fetch data");
          throw new GrillException(
              "Query aborted, unable to fetch data");
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          logger.error(e.getMessage(), e);
          throw new GrillException(e.getMessage(), e);
        }
      }

    } catch (BeeswaxException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    } catch (TException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    } catch (QueryNotFoundException e) {
      logger.error(e.getMessage(), e);
      throw new GrillException(e.getMessage(), e);

    }

    ImpalaResultSet iResultSet = new ImpalaResultSet(client, queryHandle);
    return iResultSet;

  }

  @Override
  public void configure(Configuration conf) throws GrillException {
    final String HOST = "HOST";
    final String PORT = "PORT";
    TSocket sock = new TSocket(conf.get(HOST), conf.getInt(PORT, 9999));
    try {
      sock.open();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
      throw new GrillException(e.getMessage(), e);
    }
    TBinaryProtocol protocol = new TBinaryProtocol(sock);
    this.client = new ImpalaService.Client(protocol);
    logger.info("Successfully connected to host" + conf.get(HOST) + ":"
        + conf.getInt(PORT, 9999));

  }

  @Override
  public QueryStatus getStatus(com.inmobi.grill.api.QueryHandle handle) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean cancelQuery(com.inmobi.grill.api.QueryHandle handle) {
    // TODO Auto-generated method stub
    return false;
  }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws GrillException {
		// TODO Auto-generated method stub
		
	}

  @Override
  public void closeQuery(com.inmobi.grill.api.QueryHandle arg0)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet execute(QueryContext context) throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void executeAsync(QueryContext context) throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void closeResultSet(com.inmobi.grill.api.QueryHandle handle)
      throws GrillException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void registerForCompletionNotification(
      com.inmobi.grill.api.QueryHandle handle, long timeoutMillis,
      QueryCompletionListener listener) throws GrillException {
    throw new GrillException("Not implemented");    
  }

}
