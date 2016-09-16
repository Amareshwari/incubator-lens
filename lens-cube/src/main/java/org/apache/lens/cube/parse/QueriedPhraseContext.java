package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import lombok.Data;

@Data
class QueriedPhraseContext extends TracksQueriedColumns implements TrackQueriedCubeFields {
  private final ASTNode exprAST;
  private Map<String, Set<String>> tblAliasToColumns = new HashMap<>();
  private Boolean aggregate;
  private String expr;
  private Set<String> queriedDimAttrs = new HashSet<>();
  private Set<String> queriedMsrs = new HashSet<>();
  private Set<String> queriedExprColumns = new HashSet<>();

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
  }

  @Override
  public void addQueriedMsr(String msrName) {
    queriedMsrs.add(msrName);
  }

  @Override
  public void addQueriedExprColumn(String exprCol) {
    queriedExprColumns.add(exprCol);
  }
}
