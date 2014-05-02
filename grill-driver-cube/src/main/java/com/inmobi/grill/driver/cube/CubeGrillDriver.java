package com.inmobi.grill.driver.cube;

/*
 * #%L
 * Grill Cube Driver
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import com.inmobi.grill.api.GrillException;
import com.inmobi.grill.api.query.QueryHandle;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.api.query.QueryStatus;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;
import com.inmobi.grill.server.api.driver.DriverSelector;
import com.inmobi.grill.server.api.driver.GrillDriver;
import com.inmobi.grill.server.api.driver.GrillResultSet;
import com.inmobi.grill.server.api.driver.QueryCompletionListener;
import com.inmobi.grill.server.api.query.PreparedQueryContext;
import com.inmobi.grill.server.api.query.QueryContext;

public class CubeGrillDriver implements GrillDriver {
  public static final Logger LOG = Logger.getLogger(CubeGrillDriver.class);

  private final List<GrillDriver> drivers;
  private final DriverSelector driverSelector;
  private Configuration conf;
  private Map<QueryHandle, QueryContext> queryContexts =
      new HashMap<QueryHandle, QueryContext>();
  private Map<QueryPrepareHandle, PreparedQueryContext> preparedQueries =
      new HashMap<QueryPrepareHandle, PreparedQueryContext>();


  public CubeGrillDriver(Configuration conf) throws GrillException {
    this(conf, new MinQueryCostSelector());
  }

  public CubeGrillDriver(Configuration conf, DriverSelector driverSelector)
      throws GrillException {
    this.conf = new HiveConf(conf, CubeGrillDriver.class);
    this.drivers = new ArrayList<GrillDriver>();
    loadDrivers();
    this.driverSelector = driverSelector;
  }

  private void loadDrivers() throws GrillException {
    String[] driverClasses = conf.getStrings(
        GrillConfConstants.ENGINE_DRIVER_CLASSES);
    if (driverClasses != null) {
      for (String driverClass : driverClasses) {
        try {
          Class<?> clazz = Class.forName(driverClass);
          GrillDriver driver = (GrillDriver) clazz.newInstance();
          driver.configure(conf);
          drivers.add(driver);
        } catch (Exception e) {
          LOG.warn("Could not load the driver:" + driverClass, e);
          throw new GrillException("Could not load driver " + driverClass, e);
        }
      }
    } else {
      throw new GrillException("No drivers specified");
    }
  }

  protected GrillDriver selectDriver(Map<GrillDriver,
      String> queries, Configuration conf) {
    return driverSelector.select(drivers, queries, conf);
  }

  public static class MinQueryCostSelector implements DriverSelector {
    /**
     * Returns the driver that has the minimum query cost.
     */
    @Override
    public GrillDriver select(List<GrillDriver> drivers,
        final Map<GrillDriver, String> driverQueries, final Configuration conf) {
      return Collections.min(drivers, new Comparator<GrillDriver>() {
        @Override
        public int compare(GrillDriver d1, GrillDriver d2) {
          DriverQueryPlan c1;
          DriverQueryPlan c2;
          conf.setBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN, false);
          try {
            c1 = d1.explain(driverQueries.get(d1), conf);
            c2 = d2.explain(driverQueries.get(d2), conf);
          } catch (GrillException e) {
            throw new RuntimeException("Could not compare drivers", e);
          }
          return c1.getCost().compareTo(c2.getCost());
        }
      });
    }
  }

  public GrillResultSet execute(String query, Configuration conf)
      throws GrillException {
    return execute(createQueryContext(query, conf));
  }

  @Override
  public GrillResultSet execute(QueryContext ctx) throws GrillException {
    rewriteAndSelect(ctx);
    return ctx.getSelectedDriver().execute(ctx);
  }

  private void rewriteAndSelect(QueryContext ctx) throws GrillException {
    queryContexts.put(ctx.getQueryHandle(), ctx);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries, conf);
    
    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  private QueryContext createQueryContext(String query, Configuration conf) {
    return new QueryContext(query, null, conf);
  }

  public QueryHandle executeAsync(String query, Configuration conf)
      throws GrillException {
    QueryContext ctx = createQueryContext(query, conf);
    executeAsync(ctx);
    return ctx.getQueryHandle();
  }

  @Override
  public void executeAsync(QueryContext ctx) throws GrillException {
    rewriteAndSelect(ctx);
    ctx.getSelectedDriver().executeAsync(ctx);
  }

  public QueryStatus getStatus(QueryHandle handle) throws GrillException {
    return getContext(handle).getSelectedDriver().getStatus(handle);
  }

  @Override
  public GrillResultSet fetchResultSet(QueryContext context)
      throws GrillException {
    return context.getSelectedDriver().fetchResultSet(context);
  }

  @Override
  public void configure(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws GrillException {
    return getContext(handle).getSelectedDriver().cancelQuery(handle);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void close() throws GrillException {
    for (GrillDriver driver : drivers) {
      driver.close();
    }
    drivers.clear();
    queryContexts.clear();
  }

  @Override
  public void closeQuery(QueryHandle handle) throws GrillException {
    getContext(handle).getSelectedDriver().closeQuery(handle);
    queryContexts.remove(handle);
  }

  private QueryContext getContext(QueryHandle handle)
      throws GrillException {
    QueryContext ctx = queryContexts.get(handle);
    if (ctx == null) {
      throw new GrillException("Query not found " + ctx); 
    }
    return ctx;
  }

  public List<GrillDriver> getDrivers() {
    return drivers;
  }

  private void rewriteAndSelectForPrepare(PreparedQueryContext ctx)
      throws GrillException {
    preparedQueries.put(ctx.getPrepareHandle(), ctx);
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        ctx.getUserQuery(), drivers, ctx.getConf());

    // 2. select driver to run the query
    GrillDriver driver = selectDriver(driverQueries, conf);
    
    ctx.setSelectedDriver(driver);
    ctx.setDriverQuery(driverQueries.get(driver));
  }

  @Override
  public DriverQueryPlan explain(String query, Configuration conf)
      throws GrillException {
    if (conf.getBoolean(GrillConfConstants.PREPARE_ON_EXPLAIN,
        GrillConfConstants.DEFAULT_PREPARE_ON_EXPLAIN)) {
      PreparedQueryContext ctx = new PreparedQueryContext(query, null, conf);
      return explainAndPrepare(ctx);
    }
    Map<GrillDriver, String> driverQueries = RewriteUtil.rewriteQuery(
        query, drivers, conf);
    GrillDriver driver = selectDriver(driverQueries, conf);
    return driver.explain(driverQueries.get(driver), conf);
  }

  @Deprecated
  public GrillResultSet executePrepare(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    return execute(ctx);
  }

  @Deprecated
  public void executePrepareAsync(QueryHandle handle, Configuration conf)
      throws GrillException {
    QueryPrepareHandle pHandle = new QueryPrepareHandle(handle.getHandleId());
    QueryContext ctx = new QueryContext(preparedQueries.get(pHandle), null, conf);
    ctx.setQueryHandle(handle);
    executeAsync(ctx);
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws GrillException {
    rewriteAndSelectForPrepare(pContext);
    pContext.getSelectedDriver().prepare(pContext);
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext)
      throws GrillException {
    rewriteAndSelectForPrepare(pContext);
    return pContext.getSelectedDriver().explainAndPrepare(pContext);
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle)
      throws GrillException {
    PreparedQueryContext ctx = preparedQueries.remove(handle);
    if (ctx != null) {
      ctx.getSelectedDriver().closePreparedQuery(handle);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws GrillException {
    getContext(handle).getSelectedDriver().closeResultSet(handle);
  }

  @Override
  public void registerForCompletionNotification(QueryHandle handle,
      long timeoutMillis, QueryCompletionListener listener)
      throws GrillException {
    throw new GrillException("Not implemented");
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    drivers.clear();
    int nDrivers = in.readInt();    
    for (int i = 0; i < nDrivers; i++) {
      try {
        String driverClsName = in.readUTF();
        Class<? extends GrillDriver> driverCls = 
            (Class<? extends GrillDriver>)Class.forName(driverClsName);
        GrillDriver driver = (GrillDriver) driverCls.newInstance();
        driver.configure(conf);
        driver.readExternal(in);
        drivers.add(driver);
      } catch (Exception exc) {
        throw new IOException(exc);
     }
      
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(drivers.size());
    for (GrillDriver driver : drivers) {
      out.writeUTF(driver.getClass().getName());
      driver.writeExternal(out);
    }
  }
}
