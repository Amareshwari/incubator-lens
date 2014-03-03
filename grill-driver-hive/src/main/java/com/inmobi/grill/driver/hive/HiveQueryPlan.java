package com.inmobi.grill.driver.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryHandle;
import com.inmobi.grill.api.QueryPlan;
import com.inmobi.grill.api.GrillConfUtil;

public class HiveQueryPlan extends QueryPlan {
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

  public HiveQueryPlan(List<String> explainOutput, QueryHandle queryHandle,
      HiveConf conf) throws HiveException {
    setHandle(queryHandle);
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
      //System.out.println("@@ " + states + " [" +state + "] " + line);
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
            String costStr = tbl.getParameters().get(GrillConfUtil.STORAGE_COST);
            
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
          if (tr.startsWith("expr:") && states.get(states.size() - 1) == ParserState.TABLE_SCAN) {
            numSels++;
          }
          break;
        case GROUPBY_EXPRS:
          if (tr.startsWith("expr:")) {
            numDefaultAggrExprs++;
          }
          break;
        case GROUPBY_KEYS:
          if (tr.startsWith("expr:")) {
            numGbys++;
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
