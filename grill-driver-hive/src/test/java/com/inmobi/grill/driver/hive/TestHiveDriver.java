package com.inmobi.grill.driver.hive;

import static org.testng.Assert.*;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.grill.api.GrillResultSet;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryStatus;
import com.inmobi.grill.api.QueryStatus.Status;
import com.inmobi.grill.exception.GrillException;

import con.inmobi.grill.driver.hive.EmbeddedThriftConnection;
import con.inmobi.grill.driver.hive.HiveDriver;
import con.inmobi.grill.driver.hive.HiveInMemoryResultSet;
import con.inmobi.grill.driver.hive.ThriftConnection;


public class TestHiveDriver {
	private static final String TEST_DATA_FILE = "testdata/testdata1.txt";
	private Configuration conf;
	private HiveDriver driver;
	
	@BeforeTest
	public void beforeTest() throws Exception {
		conf = new Configuration();
		conf.setClass(HiveDriver.GRILL_HIVE_CONNECTION_CLASS, EmbeddedThriftConnection.class, 
				ThriftConnection.class);
		conf.set(HiveDriver.GRILL_PASSWORD_KEY, "password");
		conf.set(HiveDriver.GRILL_USER_NAME_KEY, "user");
		conf.set("hive.lock.manager", "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
		
		driver = new HiveDriver(conf);
		System.out.println("Driver created");
	}
	
	@AfterTest
	public void afterTest() throws Exception {
		driver.close();
	}
	
	// Tests
	@Test
	public void testExecuteQuery()  throws Exception {
		System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
		final String TBL = "HIVE_DRIVER_TEST";
		
		String dropTable = "DROP TABLE IF EXISTS " + TBL;
		String createTable = "CREATE TABLE " + TBL  +"(ID STRING)";
		conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, false);
		GrillResultSet resultSet = driver.execute(dropTable, conf);
		assertNotNull(resultSet);
		assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");
		
		resultSet = driver.execute(createTable, conf);
		assertNotNull(resultSet);
		assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");
		
		// Load some data into the table
		String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + TBL;
		resultSet = driver.execute(dataLoad, conf);
	}
	
	// Check query is executed, and print results. verify results
	// executeAsync
	@Test
	public void testExecuteQueryAsync()  throws Exception {
		System.out.println("Hadoop Location: " + System.getProperty("hadoop.bin.path"));
		final String TBL = "HIVE_DRIVER_TEST";
		
		String dropTable = "DROP TABLE IF EXISTS " + TBL;
		String createTable = "CREATE TABLE " + TBL  +"(ID STRING)";
		conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, false);
		GrillResultSet resultSet = driver.execute(dropTable, conf);
		assertNotNull(resultSet);
		assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");
		
		resultSet = driver.execute(createTable, conf);
		assertNotNull(resultSet);
		assertTrue(resultSet instanceof HiveInMemoryResultSet, "expecting in-memory result set");
		
		// Load some data into the table
		String dataLoad = "LOAD DATA LOCAL INPATH '"+ TEST_DATA_FILE +"' OVERWRITE INTO TABLE " + TBL;
		QueryHandle handle = driver.executeAsync(dataLoad, conf);
		
		Set<Status> expected = 
				new LinkedHashSet<Status>(Arrays.asList(Status.RUNNING, Status.SUCCESSFUL));
		Set<Status> actualStates = new LinkedHashSet<Status>();
		
		while (true) {
			QueryStatus status = driver.getStatus(handle);
			actualStates.add(status.getStatus());
			assertNotNull(status);
			System.err.println("Status " + status);
			if (status.getStatus() == Status.SUCCESSFUL) {
				break;
			}
			Thread.sleep(100);
		}
		assertEquals(expected, actualStates);
		
		driver.closeQuery(handle);
		
		// This should throw error now
		try {
			QueryStatus status = driver.getStatus(handle);
			assertTrue(false, "Should have thrown exception");
		} catch (GrillException exc) {
			assertTrue(true);
		}
		
		// Run the command again, this time cancelling immediately
		handle = driver.executeAsync(dataLoad, conf);
		assertTrue(driver.cancelQuery(handle));
		QueryStatus status = driver.getStatus(handle);
		assertEquals(status.getStatus(), Status.CANCELED, "Query should be cancelled now");
		driver.closeQuery(handle);
		
		// Now run a command that would fail
		String expectFail = "SELECT * FROM FOO_BAR";
		conf.setBoolean(HiveDriver.GRILL_RESULT_SET_TYPE_KEY, true);
		handle = driver.executeAsync(expectFail, conf);
		
		Set<Status> terminationStates = 
				EnumSet.of(Status.CANCELED, Status.CLOSED, Status.FAILED, Status.SUCCESSFUL);
		
		while (true) {
			status = driver.getStatus(handle);
			actualStates.add(status.getStatus());
			assertNotNull(status);
			System.err.println("Status " + status);
			Status currStatus = status.getStatus();
			if (terminationStates.contains(currStatus)) {
				break;
			}
			Thread.sleep(100);
		}
		
		status = driver.getStatus(handle);
		assertEquals(status.getStatus(), Status.FAILED, "Expecting query to fail");
		driver.closeQuery(handle);
	}
	
	// explain
	
 
}
