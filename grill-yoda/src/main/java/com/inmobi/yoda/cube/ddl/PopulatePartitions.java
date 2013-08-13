package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.columnar.LazyNOBColumnarSerde;
import org.apache.hadoop.mapred.TextInputFormat;

public class PopulatePartitions {

  private final HiveConf conf;

  public PopulatePartitions(HiveConf conf) {
    this.conf = conf;
  }

  public void populateAllDimParts(Path basePath, SimpleDateFormat pathDateFormat,
      Date partitionTimestamp, boolean checkExist)
          throws HiveException, IOException {
    final CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);
    List<CubeDimensionTable> dims = client.getAllDimensionTables();
    
    // basePath = /user/yoda/warehouse/metadata/
    for (CubeDimensionTable dim : dims) {
      Path partPath = new Path(new Path(basePath, dim.getName()),
            "rq_dt=" + pathDateFormat.format(partitionTimestamp));
      if (checkExist) {
        FileSystem fs = partPath.getFileSystem(conf);
        if (!fs.exists(partPath)) {
          System.out.println("Path" + partPath +" does not exist");          
          continue;
        }
        
      }
      System.out.println("Adding partition at Path" + partPath);
      HDFSStorage storage = new HDFSStorage(CubeDDL.YODA_STORAGE,
          TextInputFormat.class.getCanonicalName(),
          HiveIgnoreKeyTextOutputFormat.class.getCanonicalName(),
          null, true, null, null, null);
      storage.setPartLocation(partPath);
      client.addPartition(dim, storage, partitionTimestamp);
    }
  }

  public void populateCubeParts(String cubeName, Date start, Date end,
      UpdatePeriod updatePeriod, Path basePath, SimpleDateFormat dateFormat,
      String summaries, boolean checkExist)
          throws HiveException, IOException, ParseException {
    final CubeMetastoreClient client = CubeMetastoreClient.getInstance(conf);

    Cube cube = client.getCube(CubeDDL.CUBE_NAME_PFX + cubeName);
    FileSystem fs = basePath.getFileSystem(conf);

    boolean populateOnlyRaw = false;
    boolean populateAll = false;
    List<String> summaryList = Arrays.asList(StringUtils.split(
        summaries.toLowerCase(), ","));
    if (summaryList.size() == 1) {
      if (summaryList.get(0).equalsIgnoreCase(CubeDDL.RAW_FACT_NAME)) {
        populateOnlyRaw = true;
      }
      if (summaryList.get(0).equalsIgnoreCase("ALL")) {
        populateAll = true;
      }
    }
    // cube.request.path=rrcube/transformationoutput2
    Path rawFactPath = new Path(basePath, cube.getProperties().get(
        "cube." + cubeName + ".path"));
    //cube.request.summaries.path=rrcube
    Path summariesPath = null;
    if (!populateOnlyRaw) {
      String sumPath = cube.getProperties().get(
          "cube." + cubeName + ".summaries.path");
      if (sumPath != null) {
        summariesPath = new Path(basePath, cube.getProperties().get(
            "cube." + cubeName + ".summaries.path"));
      } else {
        System.out.println("summaries.path not available, populating only raw");
        populateOnlyRaw = true;
      }
    }

    List<CubeFactTable> facts = client.getAllFactTables(cube);
    Calendar cal = Calendar.getInstance();
    cal.setTime(start);
    Date dt = cal.getTime();
    while (!dt.after(end)) {
      // for each fact add the partition for dt
      for (CubeFactTable fact : facts) {
        String factPathName = fact.getName().substring(cubeName.length() + 1);
        if (!populateAll && !summaryList.contains(factPathName)) {
          continue; 
        }
        if (populateOnlyRaw && (!factPathName.equalsIgnoreCase(
            CubeDDL.RAW_FACT_NAME))) {
          continue;
        }
        for (Map.Entry<String, Set<UpdatePeriod>> entry : 
          fact.getUpdatePeriods().entrySet()) {
          if (!entry.getValue().contains(updatePeriod)) {
            continue;
          }

          Path partPath;
          if (factPathName.equalsIgnoreCase(CubeDDL.RAW_FACT_NAME)) {
            partPath = new Path(rawFactPath, dateFormat.format(dt));
          } else {
            partPath = new Path(new Path(new Path(summariesPath, factPathName),
                updatePeriod.name().toLowerCase()), dateFormat.format(dt));
          }
          if (checkExist && !fs.exists(partPath)) {
            System.out.println("Path" + partPath +" does not exist");
            continue;
          }
          System.out.println("Adding partitions for Path" + partPath);
          if (entry.getKey().equalsIgnoreCase(CubeDDL.YODA_STORAGE)) {
            HDFSStorage storage = new HDFSStorage(entry.getKey(),
                RCFileInputFormat.class.getCanonicalName(),
                RCFileOutputFormat.class.getCanonicalName(),
                LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
                null);
            storage.setPartLocation(partPath);
            Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
            partitionTimestamps.put(CubeDDL.PART_KEY_IT, dt);
            client.addPartition(fact, storage, updatePeriod, partitionTimestamps);
          } else if (entry.getKey().equalsIgnoreCase(CubeDDL.YODA_PIE_STORAGE)) {
            Date pt = dt;
            FileStatus[] iStats = fs.listStatus(partPath);
            for (FileStatus istat : iStats) {
              Date it = dateFormat.parse(istat.getPath().getName());
              FileStatus[] eStats = fs.listStatus(istat.getPath());
              for (FileStatus estat : eStats) {
                Date et = dateFormat.parse(estat.getPath().getName());
                HDFSStorage storage = new HDFSStorage(entry.getKey(),
                    RCFileInputFormat.class.getCanonicalName(),
                    RCFileOutputFormat.class.getCanonicalName(),
                    LazyNOBColumnarSerde.class.getCanonicalName(), true, null, null,
                    null);
                storage.setPartLocation(estat.getPath());
                Map<String, Date> partitionTimestamps = new HashMap<String, Date>();
                partitionTimestamps.put(CubeDDL.PART_KEY_PT, pt);
                partitionTimestamps.put(CubeDDL.PART_KEY_IT, it);
                partitionTimestamps.put(CubeDDL.PART_KEY_ET, et);
                client.addPartition(fact, storage, updatePeriod, partitionTimestamps);
              }
            }
          }
        }
      }
      cal.add(updatePeriod.calendarField(), 1);
      dt = cal.getTime();
    }    
  }

  public static void main(String[] args)
      throws HiveException, ParseException, IOException {
    if (args.length < 4) {
      System.out.println("Usage:" +
          "\t [-dims basepath timestamp pathDateFormat]\n" +
          "\t [cubeName startPartition endPartition" +
          " UpdatePeriod basePath pathDateFormat (summarylist|raw|all) ]");
      return;
    }
    HiveConf conf = new HiveConf(PopulatePartitions.class);
    PopulatePartitions pp = new PopulatePartitions(conf);
    if (args[0].equalsIgnoreCase("-dims")) {
      String baseDimPath = args[1];
      String dimTS = args[2];
      String pathDateFormat = args[3];
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      pp.populateAllDimParts(new Path(baseDimPath), dateFormat,
          dateFormat.parse(dimTS), true);
    } else {
      String cubeName = args[0];
      String startPos = args[1];
      String endPos = args[2];
      String updatePeriod = args[3];
      String basePath = args[4];
      String pathDateFormat = args[5];
      String summaries = args[6];

      UpdatePeriod p = UpdatePeriod.valueOf(updatePeriod.toUpperCase());
      SimpleDateFormat dateFormat = new SimpleDateFormat(pathDateFormat);
      Date start = dateFormat.parse(startPos);
      Date end = dateFormat.parse(endPos);

      pp.populateCubeParts(cubeName, start, end, p, new Path(basePath),
          dateFormat, summaries, true);
    }
  }
}
