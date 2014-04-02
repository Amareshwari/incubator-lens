package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.inmobi.grill.api.query.QueryCost;
import com.inmobi.grill.api.query.QueryPrepareHandle;
import com.inmobi.grill.server.api.GrillConfConstants;
import com.inmobi.grill.server.api.driver.DriverQueryPlan;

public class HiveQueryPlan extends DriverQueryPlan {
  private String explainOutput;
  enum ParserState {
    BEGIN,
    FILE_OUTPUT_OPERATOR,
    TABLE_SCAN,
    JOIN,
    SELECT,
    GROUPBY,
    GROUPBY_KEYS,
    GROUPBY_EXPRS,
    MOVE,
    MAP_REDUCE,
  };

  public HiveQueryPlan(List<String> explainOutput, QueryPrepareHandle prepared,
      HiveConf conf) throws HiveException {
    setPrepareHandle(prepared);
    setExecMode(ExecMode.BATCH);
    setScanMode(ScanMode.PARTIAL_SCAN);
    extractPlanDetails(explainOutput, conf);
    this.explainOutput = StringUtils.join(explainOutput, '\n');
  }

  private void extractPlanDetails(List<String> explainOutput, HiveConf conf) throws HiveException {
    ParserState state = ParserState.BEGIN;
    ParserState prevState = state;
    ArrayList<ParserState> states = new ArrayList<ParserState>();
    Hive metastore = Hive.get(conf);

    for (String line : explainOutput) {
      String tr = line.trim();
      prevState = state;
      state = nextState(tr, state);

      if (prevState != state) {
        states.add(prevState);
      }

      switch (state) {
        case MOVE:
          if (tr.startsWith("destination:")) {
            String outputPath = tr.replace("destination:", "").trim();
            resultDestination = outputPath;
          }
          break;
        case TABLE_SCAN:
          if (tr.startsWith("alias:")) {
            String tableName = tr.replace("alias:", "").trim();
            tablesQueried.add(tableName);
            Table tbl = metastore.getTable(tableName);
            String costStr = tbl.getParameters().get(GrillConfConstants.STORAGE_COST);
            
            Double weight = 1d;
            if (costStr != null) {
              weight = Double.parseDouble(costStr);
            }
            tableWeights.put(tableName, weight);
          }
          break;
        case JOIN:
          if (tr.equals("condition map:")) {
            numJoins++;
          }
          break;
        case SELECT:
          if (tr.startsWith("expressions:") && states.get(states.size() - 1) == ParserState.TABLE_SCAN) {
            numSels += StringUtils.split(tr, ",").length;
          }
          break;
        case GROUPBY_EXPRS:
          if (tr.startsWith("aggregations:")) {
            numAggrExprs += StringUtils.split(tr, ",").length;
          }
          break;
        case GROUPBY_KEYS:
          if (tr.startsWith("keys:")) {
            numGbys += StringUtils.split(tr, ",").length;
          }
          break;
      }
    }
	}

  private ParserState nextState(String tr, ParserState state) {
    if (tr.equals("File Output Operator")) {
      return ParserState.FILE_OUTPUT_OPERATOR;
    } else if (tr.equals("Map Reduce")) {
      return ParserState.MAP_REDUCE;
    } else if (tr.equals("Move Operator")) {
      return ParserState.MOVE;
    } else if (tr.equals("TableScan")) {
      return ParserState.TABLE_SCAN;
    } else if (tr.equals("Map Join Operator")) {
      return ParserState.JOIN;
    } else if (tr.equals("Select Operator")) {
      return ParserState.SELECT;
    } else if (tr.equals("Group By Operator")) {
      return ParserState.GROUPBY;
    } else if (tr.startsWith("aggregations:") && state == ParserState.GROUPBY) {
      return ParserState.GROUPBY_EXPRS;
    } else if (tr.startsWith("keys:") && state == ParserState.GROUPBY_EXPRS) {
      return ParserState.GROUPBY_KEYS;
    }

    return state;
  }

  @Override
	public String getPlan() {
		return explainOutput;
	}

	@Override
	public QueryCost getCost() {
		return null;
	}

}
