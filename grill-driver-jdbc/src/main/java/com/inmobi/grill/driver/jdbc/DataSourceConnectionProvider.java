package com.inmobi.grill.driver.jdbc;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceConnectionProvider implements ConnectionProvider {
  public static final Logger LOG = Logger.getLogger(DataSourceConnectionProvider.class);
  private Map<DriverConfig, ComboPooledDataSource> dataSourceMap;
  
  public DataSourceConnectionProvider() {
    dataSourceMap = new HashMap<DriverConfig, ComboPooledDataSource>();
  }
  
  public DriverConfig getDriverConfigfromConf(Configuration conf) {
    return new DriverConfig(
        conf.get(JDBCDriverConfConstants.JDBC_DRIVER_CLASS),
        conf.get(JDBCDriverConfConstants.JDBC_DB_URI),
        conf.get(JDBCDriverConfConstants.JDBC_USER),
        conf.get(JDBCDriverConfConstants.JDBC_PASSWORD)
        );
  }
  
  protected class DriverConfig {
    final String driverClass;
    final String jdbcURI;
    final String user;
    final String password;
    boolean hasHashCode = false;
    int hashCode;
    
    public DriverConfig(String driverClass, String jdbcURI, String user, String password) {
      this.driverClass = driverClass;
      this.jdbcURI = jdbcURI;
      this.user = user;
      this.password = password;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DriverConfig)) {
        return false;
      }
      
      DriverConfig other = (DriverConfig) obj;
      
      return driverClass.equals(other.driverClass) &&
          jdbcURI.equals(other.jdbcURI) &&
          user.equals(other.user) &&
          password.equals(other.password);
    }
    
    @Override
    public int hashCode() {
      if (!hasHashCode) {
        StringBuilder builder = 
            new StringBuilder(driverClass).append(jdbcURI).append(user).append(password);
        hashCode = builder.toString().hashCode();
        hasHashCode = true;
      }
      return hashCode;
    }
    
    @Override
    public String toString() {
      StringBuilder builder = 
          new StringBuilder("jdbcDriverClass: ").append(driverClass).append(", uri: ")
          .append(jdbcURI).append(", user: ")
          .append(user);
      return builder.toString();
    }
    
  }
  
  @Override
  public synchronized Connection getConnection(Configuration conf) throws SQLException {
    DriverConfig config = getDriverConfigfromConf(conf);
    if (!dataSourceMap.containsKey(config)) {
      ComboPooledDataSource cpds = new ComboPooledDataSource();
      try {
        cpds.setDriverClass(config.driverClass );
      } catch (PropertyVetoException e) {
        throw new IllegalArgumentException("Unable to set driver class:" + config.driverClass, e);
      }
      cpds.setJdbcUrl(config.jdbcURI);
      cpds.setUser(config.jdbcURI);                                  
      cpds.setPassword(config.password);                                  
      
      // Maximum number of connections allowed in the pool
      cpds.setMaxPoolSize(conf.getInt(JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE, 
          JDBCDriverConfConstants.JDBC_POOL_MAX_SIZE_DEFAULT));
      // Max idle time before a connection is closed
      cpds.setMaxIdleTime(conf.getInt(JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME, 
          JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME_DEFAULT));
      // Max idel time before connection is closed if 
      // number of connections is > min pool size (default = 3)
      cpds.setMaxIdleTimeExcessConnections(
          conf.getInt(JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME, 
              JDBCDriverConfConstants.JDBC_POOL_IDLE_TIME_DEFAULT));
      // Maximum number of prepared statements to cache per connection
      cpds.setMaxStatementsPerConnection(
          conf.getInt(JDBCDriverConfConstants.JDBC_MAX_STATEMENTS_PER_CONNECTION, 
              JDBCDriverConfConstants.JDBC_MAX_STATEMENTS_PER_CONNECTION_DEFAULT));
      dataSourceMap.put(config, cpds);
      LOG.info("Created new datasource for config: " + config);
    }
    return dataSourceMap.get(config).getConnection();
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<DriverConfig, ComboPooledDataSource> entry : dataSourceMap.entrySet()) {
      entry.getValue().close();
      LOG.info("Closed datasource: " + entry.getKey());
    }
    dataSourceMap.clear();
    LOG.info("Closed datasource connection provider");
  }

}
