package com.inmobi.yoda.hive;


import com.google.common.io.Files;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestForwardingUDF {
  public static final String TEST_TBL = "udfTest.test_yoda_udf_table";
  private HiveConf conf;
  private ThriftCLIServiceClient hiveClient;
  private SessionHandle session;
  private Map<String, String> confOverlay;
  private File testFile;
  public static final int NUM_LINES = 100;

  @BeforeClass
  public void registerUDFS() throws Exception {
    UdfRegistry.registerUDFS();
  }

  @BeforeTest
  public void setup() throws Exception {
    conf = new HiveConf(TestForwardingUDF.class);
    conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    hiveClient = new ThriftCLIServiceClient(new EmbeddedThriftBinaryCLIService());
    session = hiveClient.openSession(conf.getUser(), "");
    confOverlay = new HashMap<String, String>();
    hiveClient.executeStatement(session, "CREATE DATABASE udfTest", confOverlay);
    hiveClient.executeStatement(session, "SET hive.lock.manager=org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager",
      confOverlay);
    //hiveClient.executeStatement(session, "DROP TABLE IF EXISTS " + TEST_TBL, confOverlay);
    hiveClient.executeStatement(session, "CREATE TABLE " + TEST_TBL + "(ID STRING, CSCDT STRING)" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'", confOverlay);

    DateTimeFormatter dateFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();
    // Create test input
    PrintWriter out = null;
    try {
      testFile = new File("target/grill_yoda_test.txt");
      out = new PrintWriter(testFile);
      for (int i = 0; i < NUM_LINES; i++) {
        out.print(i + "\t");
        dateFmt.printTo(out, new Date().getTime());
        dateFmt.print(new Date().getTime());
        out.print('\n');
      }
    } catch (Exception ex) {
      throw ex;
    } finally {
       if (out != null) {
         out.close();
       }
    }

    String testFilePath = testFile.getPath();
    hiveClient.executeStatement(session,
      "LOAD DATA LOCAL INPATH '" + testFilePath + "' OVERWRITE INTO TABLE " + TEST_TBL,
      confOverlay);

  }

  @Test
  public void testUdfForward() throws Exception {
    OperationHandle opHandle = hiveClient.executeStatement(session,
      "INSERT OVERWRITE LOCAL DIRECTORY 'target/udfTestOutput' " +
        "SELECT str_yoda_udf('com.inmobi.dw.yoda.udfs.generic.MD5', ID), " +
        "bool_yoda_udf('com.inmobi.dw.yoda.udfs.custom.csc.BeforeCSCDateUDF', CSCDT)," +
        "truncate_bucket('2013-01-01 01:01:01.001', 'week_of_year') FROM " + TEST_TBL,
      confOverlay);
    File outputDir = new File("target/udfTestOutput");
    List<String> lines = new ArrayList<String>();
    for (File f : outputDir.listFiles()) {
      if (!f.getName().endsWith(".crc")) {
        lines.addAll(Files.readLines(f, Charset.defaultCharset()));
      }
    }
    assertEquals(lines.size(), NUM_LINES);
  }

  @Test
  public void testTruncateBucketUDF() throws Exception {
    assertEquals(new TruncateBucket().evaluate("2013-01-01 01:01:01.001", "WEEK_OF_YEAR"), "01");
    try {
      new TruncateBucket().evaluate("2013-01-01 01:01:01.001", "WEEK_OF_YEAR_111");
      fail("Should have thrown error");
    } catch (IllegalArgumentException exc) {

    }
  }

  @AfterTest
  public void tearDown() throws Exception {
    if (testFile != null)  {
      testFile.delete();
    }
    Hive hive = Hive.get(conf);
    hive.dropTable(TEST_TBL);
    hive.dropDatabase("udfTest");
    hiveClient.closeSession(session);
  }
}
