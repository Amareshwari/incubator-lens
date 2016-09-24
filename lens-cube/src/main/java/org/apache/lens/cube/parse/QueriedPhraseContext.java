package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.lens.cube.metadata.MetastoreConstants;
import org.apache.lens.cube.metadata.TimeRange;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Data
@EqualsAndHashCode(callSuper = false)
@Slf4j
class QueriedPhraseContext extends TracksQueriedColumns implements TrackQueriedCubeFields {
  private final ASTNode exprAST;
  private Map<String, Set<String>> tblAliasToColumns = new HashMap<>();
  private Boolean aggregate;
  private String expr;
  private Set<String> queriedDimAttrs = new HashSet<>();
  private Set<String> queriedMsrs = new HashSet<>();
  private Set<String> queriedExprColumns = new HashSet<>();
  private Set<String> columns = new HashSet<>();

  void setNotAggregate() {
    this.aggregate = false;
  }

  boolean isAggregate() {
    if (aggregate == null) {
      aggregate = HQLParser.hasAggregate(exprAST);
    }
    return aggregate;
  }

  public void addColumnsQueried(String tblAlias, String column) {

    Set<String> cols = tblAliasToColumns.get(tblAlias.toLowerCase());
    if (cols == null) {
      cols = new LinkedHashSet<>();
      tblAliasToColumns.put(tblAlias.toLowerCase(), cols);
    }
    cols.add(column);
  }

  String getExpr() {
    if (expr == null) {
      expr = HQLParser.getString(getExprAST()).trim();
    }
    return expr;
  }

  void updateExprs() {
    expr = HQLParser.getString(getExprAST()).trim();
  }

  @Override
  public void addQueriedDimAttr(String attrName) {
    queriedDimAttrs.add(attrName);
    columns.add(attrName);
  }

  @Override
  public void addQueriedMsr(String msrName) {
    queriedMsrs.add(msrName);
    columns.add(msrName);
  }

  @Override
  public void addQueriedExprColumn(String exprCol) {
    queriedExprColumns.add(exprCol);
    columns.add(exprCol);
  }

  public boolean hasMeasures(CubeQueryContext cubeQl) {
    if (!queriedMsrs.isEmpty()) {
      return true;
    }
    if (!queriedExprColumns.isEmpty()) {
      for (String exprCol : queriedExprColumns) {
        if (cubeQl.getQueriedExprsWithMeasures().contains(exprCol)) {
          return true;
        }
      }
    }
    return false;
  }

  boolean isEvaluable(CubeQueryContext cubeQl, CandidateFact cfact) throws LensException {
    // all dim-attributes should be present.
    for (String col : queriedDimAttrs) {
      if (!cfact.getColumns().contains(col.toLowerCase())) {
        // check if it available as reference
        if (!cubeQl.getDeNormCtx().addRefUsage(cfact, col, cubeQl.getCube().getName())) {
          log.info("Not considering fact table:{} as column {} is not available", cfact, col);
          return false;
        }
      } else if (!isFactColumnValidForRange(cubeQl, cfact, col)) {
        log.info("Not considering fact table:{} as column {} is not available in range queried", cfact, col);
        return false;
      }
    }

    // all expression columns should be evaluable
    for (String exprCol : queriedExprColumns) {
      if (!cubeQl.getExprCtx().isEvaluable(exprCol, cfact)) {
        log.info("Not considering fact table:{} as expression {} is not evaluatable", cfact, expr);
        return false;
      }
    }

    // all measures should be present
    for (String msr : queriedMsrs) {
      if (!checkForColumnExistsAndValidForRange(cfact, msr, cubeQl)) {
        return false;
      }
    }
    return true;
  }

  public static boolean isColumnAvailableInRange(final TimeRange range, Date startTime, Date endTime) {
    return (isColumnAvailableFrom(range.getFromDate(), startTime)
      && isColumnAvailableTill(range.getToDate(), endTime));
  }

  public static boolean isColumnAvailableFrom(@NonNull final Date date, Date startTime) {
    return (startTime == null) || date.equals(startTime) || date.after(startTime);
  }

  public static boolean isColumnAvailableTill(@NonNull final Date date, Date endTime) {
    return (endTime == null) || date.equals(endTime) || date.before(endTime);
  }

  public static boolean isFactColumnValidForRange(CubeQueryContext cubeql, CandidateTable cfact, String col) {
    for(TimeRange range : cubeql.getTimeRanges()) {
      if (!isColumnAvailableInRange(range, getFactColumnStartTime(cfact, col), getFactColumnEndTime(cfact, col))) {
        return false;
      }
    }
    return true;
  }

  public static Date getFactColumnStartTime(CandidateTable table, String factCol) {
    Date startTime = null;
    if (table instanceof CandidateFact) {
      for (String key : ((CandidateFact) table).fact.getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_START_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_START_TIME_PFX);
          if (factCol.equals(propCol)) {
            startTime = ((CandidateFact) table).fact.getDateFromProperty(key, false, true);
          }
        }
      }
    }
    return startTime;
  }

  public static Date getFactColumnEndTime(CandidateTable table, String factCol) {
    Date endTime = null;
    if (table instanceof CandidateFact) {
      for (String key : ((CandidateFact) table).fact.getProperties().keySet()) {
        if (key.contains(MetastoreConstants.FACT_COL_END_TIME_PFX)) {
          String propCol = StringUtils.substringAfter(key, MetastoreConstants.FACT_COL_END_TIME_PFX);
          if (factCol.equals(propCol)) {
            endTime = ((CandidateFact) table).fact.getDateFromProperty(key, false, true);
          }
        }
      }
    }
    return endTime;
  }

  static boolean checkForColumnExistsAndValidForRange(CandidateTable table, String column, CubeQueryContext cubeql) {
    return (table.getColumns().contains(column) &&  isFactColumnValidForRange(cubeql, table, column));
  }
}
