package com.inmobi.grill.driver.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.server.HiveServer2;
import org.testng.annotations.*;

import static org.testng.Assert.*;

public class TestRemoteHiveDriver extends TestHiveDriver {
  public static final Log LOG = LogFactory.getLog(TestRemoteHiveDriver.class);
  static final String HS2_HOST = "localhost";
  static final int  HS2_PORT = 12345;
  static HiveServer2 server;
  private static boolean reconnectCalled = false;

  public static class RemoteThriftConnectionTest extends RemoteThriftConnection {
    @Override
    public void setNeedsReconnect() {
      super.setNeedsReconnect();
      reconnectCalled = true;
    }
  }

  @BeforeClass
  public static void createHS2Service() throws Exception {
    conf = new HiveConf(TestRemoteHiveDriver.class);
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);
    conf = new HiveConf();
    conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, RemoteThriftConnectionTest.class,
      ThriftConnection.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    conf.set("hive.server2.thrift.bind.host", HS2_HOST);
    conf.setInt("hive.server2.thrift.port", HS2_PORT);

    SessionState.start(conf);
    Hive client = Hive.get(conf);
    Database database = new Database();
    database.setName(TestRemoteHiveDriver.class.getSimpleName());
    client.createDatabase(database, true);
    SessionState.get().setCurrentDatabase(TestRemoteHiveDriver.class.getSimpleName());

    server = new HiveServer2();
    server.init(conf);
    server.start();
    // TODO figure out a better way to wait for thrift service to start
    Thread.sleep(5000);
  }

  @AfterClass
  public static void stopHS2Service() throws Exception  {
    try {
      server.stop();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // After stopping server we expect any calls through hive server to throw TException which should reset connection
    try {
      HiveDriver driver = new HiveDriver();
      driver.configure(conf);
      conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
      conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
      try {
        driver.execute("USE " + TestRemoteHiveDriver.class.getSimpleName(), conf);
      } catch(Exception exc) {
        LOG.error("Error in reconnect test", exc);
      } finally {
        assertTrue(reconnectCalled, "Should have reset connection");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertTrue(reconnectCalled, "Execute after server stop should have called connection reset");
    Hive.get(conf).dropDatabase(TestRemoteHiveDriver.class.getSimpleName(), true, true, true);
  }

  @BeforeMethod
  @Override
  public void beforeTest() throws Exception {
    // Check if hadoop property set
    System.out.println("###HADOOP_PATH " + System.getProperty("hadoop.bin.path"));
    assertNotNull(System.getProperty("hadoop.bin.path"));
    driver = new HiveDriver();
    driver.configure(conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, false);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, false);
    driver.execute("USE " + TestRemoteHiveDriver.class.getSimpleName(), conf);
    conf.setBoolean(HiveDriver.GRILL_ADD_INSERT_OVEWRITE, true);
    conf.setBoolean(HiveDriver.GRILL_PERSISTENT_RESULT_SET, true);
  }

  @AfterMethod
  @Override
  public void afterTest() throws Exception {
    driver.close();
  }
}
