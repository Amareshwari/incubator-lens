/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse.join;

import static org.apache.lens.cube.parse.HQLParser.*;

import java.util.*;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.*;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import org.antlr.runtime.CommonToken;

import lombok.extern.slf4j.Slf4j;

/**
 * Join context related to Bridge tables
 */
@Slf4j
public class BridgeTableJoinContext {
  private final String bridgeTableFieldAggr;
  private final CubeQueryContext cubeql;
  private final CandidateFact fact;
  private final QueryAST queryAST;
  private final boolean doFlatteningEarly;
  private boolean initedBridgeClauses = false;
  private final StringBuilder bridgeSelectClause = new StringBuilder();
  private final StringBuilder bridgeFromClause = new StringBuilder();
  private final StringBuilder bridgeFilterClause = new StringBuilder();
  private final StringBuilder bridgeJoinClause = new StringBuilder();
  private final StringBuilder bridgeGroupbyClause = new StringBuilder();
  private final AliasDecider aliasDecider = new DefaultAliasDecider("balias");
  private final List<String> selectedBridgeExprs = new ArrayList<>();

  @Data
  private static class BridgeTableExprCtx {
    private final ASTNode dotAST;
    private final String alias;
  }
  private final HashMap<HashableASTNode, BridgeTableExprCtx> exprToDotAST = new HashMap<>();

  public BridgeTableJoinContext(CubeQueryContext cubeql, CandidateFact fact, QueryAST queryAST,
    String bridgeTableFieldAggr, boolean doFlatteningEarly) {
    this.cubeql = cubeql;
    this.queryAST = queryAST;
    this.fact = fact;
    this.bridgeTableFieldAggr = bridgeTableFieldAggr;
    this.doFlatteningEarly = doFlatteningEarly;
  }

  public void resetContext() {
    initedBridgeClauses = false;
    bridgeSelectClause.setLength(0);
    bridgeFromClause.setLength(0);
    bridgeFilterClause.setLength(0);
    bridgeJoinClause.setLength(0);
    bridgeGroupbyClause.setLength(0);
  }

  public void initBridgeClauses(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable, String
    userFilter,
    String storageFilter) {
    // we just found a bridge table in the path we need to initialize the clauses for subquery required for
    // aggregating fields of bridge table
    // initialize select clause with join key
    bridgeSelectClause.append(" (select ").append(toAlias).append(".").append(rel.getToColumn()).append(" as ")
      .append(rel.getToColumn());
    // group by join key
    bridgeGroupbyClause.append(" group by ").append(toAlias).append(".").append(rel.getToColumn());
    // from clause with bridge table
    bridgeFromClause.append(" from ").append(toTable.getStorageString(toAlias));
    // we need to initialize filter clause with user filter clause or storage filter if applicable
    if (StringUtils.isNotBlank(userFilter)) {
      bridgeFilterClause.append(userFilter);
    }
    if (StringUtils.isNotBlank(storageFilter)) {
      if (StringUtils.isNotBlank(bridgeFilterClause.toString())) {
        bridgeFilterClause.append(" and ");
      }
      bridgeFilterClause.append(storageFilter);
    }
    // initialize final join clause
    bridgeJoinClause.append(" on ").append(fromAlias).append(".")
      .append(rel.getFromColumn()).append(" = ").append("%s")
      .append(".").append(rel.getToColumn());
    initedBridgeClauses = true;
  }

  // if any relation has bridge table, the clause becomes the following :
  // join (" select " + joinkey + " aggr over fields from bridge table + from bridgeTable + [where user/storage
  // filters] + groupby joinkey) on joincond"
  // Or
  // " join (select " + joinkey + " aggr over fields from table reached through bridge table + from bridge table
  // join <next tables> on join condition + [and user/storage filters] + groupby joinkey) on joincond
  public void updateBridgeClause(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable,
    String userFilter, String storageFilter) {
    if (!initedBridgeClauses) {
      initBridgeClauses(rel, fromAlias, toAlias, toTable, userFilter, storageFilter);
    } else {
      addAnotherJoinClause(rel, fromAlias, toAlias, toTable, userFilter, storageFilter);
    }
  }

  public void addAnotherJoinClause(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable,
    String userFilter, String storageFilter) {
    // if bridge clauses are already inited, this is a next table getting joined with bridge table
    // we will append a simple join clause
    bridgeFromClause.append(" join ");
    bridgeFromClause.append(toTable.getStorageString(toAlias));
    bridgeFromClause.append(" on ").append(fromAlias).append(".")
      .append(rel.getFromColumn()).append(" = ").append(toAlias)
      .append(".").append(rel.getToColumn());

    if (StringUtils.isNotBlank(userFilter)) {
      bridgeFromClause.append(" and ").append(userFilter);
    }
    if (StringUtils.isNotBlank(storageFilter)) {
      bridgeFromClause.append(" and ").append(storageFilter);
    }
  }

  public String generateJoinClause(String joinTypeStr, String toAlias) throws LensException {
    StringBuilder clause = new StringBuilder(joinTypeStr);
    clause.append(" join ");
    clause.append(bridgeSelectClause.toString());
    Set<String> toCols = cubeql.getTblAliasToColumns().get(toAlias);
    // iterate over all select expressions and add them for select clause if do_flattening_early is disabled
    if (!doFlatteningEarly) {
      processSelectAST(queryAST.getSelectAST(), toAlias, toCols, bridgeTableFieldAggr);
      processWhereClauses(fact, toAlias, toCols);
      processGroupbyAST(queryAST.getGroupByAST(), toAlias, toCols);
      processOrderbyAST(queryAST.getOrderByAST(), toAlias, toCols);
      clause.append(",").append(StringUtils.join(selectedBridgeExprs, ","));
    } else {
      for (String col : cubeql.getTblAliasToColumns().get(toAlias)) {
        clause.append(",").append(bridgeTableFieldAggr).append("(").append(toAlias)
          .append(".").append(col)
          .append(")")
          .append(" as ").append(col);
      }
    }
    String bridgeFrom = bridgeFromClause.toString();
    clause.append(bridgeFrom);
    String bridgeFilter = bridgeFilterClause.toString();
    boolean addedWhere = false;
    if (StringUtils.isNotBlank(bridgeFilter)) {
      if (bridgeFrom.contains(" join ")) {
        clause.append(" and ");
      } else {
        clause.append(" where ");
        addedWhere = true;
      }
      clause.append(bridgeFilter);
    }
    clause.append(bridgeGroupbyClause.toString());
    clause.append(") ").append(toAlias);
    clause.append(String.format(bridgeJoinClause.toString(), toAlias));
    return clause.toString();
  }

  private List<String> processSelectAST(ASTNode selectAST, String tableAlias, Set<String> cols, String
    bridgeTableFieldAggr)
    throws LensException {
    // iterate over children
    for (int i = 0; i < selectAST.getChildCount(); i++) {
      ASTNode selectExprNode = (ASTNode) selectAST.getChild(i);
      ASTNode child = (ASTNode)selectExprNode.getChild(0);
      if (hasBridgeCol(child, tableAlias, cols)) {
        selectExprNode.setChild(0, getDotASTForExprAST(child, tableAlias).dotAST);
      }
    }
    return selectedBridgeExprs;
  }

  private BridgeTableExprCtx getDotASTForExprAST(ASTNode child, String tableAlias) {
    HashableASTNode hashAST = new HashableASTNode(child);
    if (!exprToDotAST.containsKey(hashAST)) {
      // add selected expression to get selected from bridge table, with a generated alias
      String colAlias = aliasDecider.decideAlias(child);
      selectedBridgeExprs.add(bridgeTableFieldAggr + "(" + HQLParser.getString(child) + ") as " + colAlias);

      // replace bridge expression with tableAlias.colAlias.
      ASTNode dot = HQLParser.getDotAST(tableAlias, colAlias);
      exprToDotAST.put(hashAST, new BridgeTableExprCtx(dot, colAlias));
    }
    return exprToDotAST.get(hashAST);
  }
  // process groupby
  private void processGroupbyAST(ASTNode ast, String tableAlias, Set<String> cols)
    throws LensException {
    if (ast == null) {
      return;
    }
    // iterate over children
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode exprNode = (ASTNode) ast.getChild(i);
      if (hasBridgeCol(exprNode, tableAlias, cols)) {
        ast.setChild(i, getDotASTForExprAST(exprNode, tableAlias).dotAST);
      }
    }
  }

  // process orderby
  private void processOrderbyAST(ASTNode ast, String tableAlias, Set<String> cols)
    throws LensException {
    if (ast == null) {
      return;
    }
    log.info("Orderby ASTdump:{}", ast.dump());
    // iterate over children
    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode exprNode = (ASTNode) ast.getChild(i);
      ASTNode child = (ASTNode)exprNode.getChild(0);
      if (hasBridgeCol(child, tableAlias, cols)) {
        exprNode.setChild(0, getDotASTForExprAST(child, tableAlias).dotAST);
      }
    }
    log.info("Orderby ASTdump:{}", ast.dump());
  }

  void processWhereClauses(CandidateFact fact, String tableAlias, Set<String> cols) throws LensException {

    for (Map.Entry<String, ASTNode> whereEntry : fact.getStorgeWhereClauseMap().entrySet()) {
      ASTNode whereAST = whereEntry.getValue();
      log.info("Where ASTdump:{}", whereAST.dump());
      processWhereAST(whereAST, tableAlias, cols, whereAST, 0);
      log.info("Where ASTdump:{}", whereAST.dump());
    }
  }

  private void processWhereAST(ASTNode ast, String tableAlias, Set<String> cols, ASTNode parent, int childPos)
    throws LensException {
    if (ast == null) {
      return;
    }
    ASTNode child;
    if (isPrimitiveBooleanExpression(ast)) {
      log.info("Primitive boolean ASTdump {}", ast.dump());
      child = (ASTNode)ast.getChild(0);
      log.info("Child ASTdump {}", child.dump());
      if (hasBridgeCol(child, tableAlias, cols)) {
        ast.setChild(0, getDotASTForExprAST(child, tableAlias).dotAST);
        parent.setChild(childPos, replaceDirectFiltersWithArrayFilter(ast));
      }
    } else if (isPrimitiveBooleanFunction(ast)) {
      log.info("Primitive boolean ASTdump {}", ast.dump());
      child = (ASTNode)ast.getChild(1);
      log.info("Child ASTdump {}", child.dump());
      if (hasBridgeCol(child, tableAlias, cols)) {
        ast.setChild(1, getDotASTForExprAST(child, tableAlias).dotAST);
        parent.setChild(childPos, replaceDirectFiltersWithArrayFilter(ast));
      }
    }
    // recurse down
    for (int i = 0; i < ast.getChildCount(); i++) {
      processWhereAST((ASTNode) ast.getChild(i), tableAlias, cols, ast, i);
    }

  }

  static ASTNode replaceDirectFiltersWithArrayFilter(ASTNode ast)
    throws LensException {
    StringBuilder arrayFilter = new StringBuilder();
    if ((ast.getType() == HiveParser.EQUAL || ast.getType() == HiveParser.EQUAL_NS)) {
      String colStr = getString((ASTNode) ast.getChild(0));
      if (ast.getType() == HiveParser.EQUAL_NS) {
        arrayFilter.append(" NOT ");
      }
      arrayFilter.append("array_contains(");
      arrayFilter.append(colStr).append(",");
      arrayFilter.append(ast.getChild(1).getText());
      arrayFilter.append(")");
    } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
      String colStr = getString((ASTNode) ast.getChild(1));
      arrayFilter.append("(");
      for (int i = 2; i < ast.getChildCount(); i++) {
        arrayFilter.append("array_contains(");
        arrayFilter.append(colStr).append(",");
        arrayFilter.append(ast.getChild(i).getText());
        arrayFilter.append(")");
        if (i + 1 != ast.getChildCount()) {
          arrayFilter.append(" OR ");
        }
      }
      arrayFilter.append(")");
    }
    String finalFilter = arrayFilter.toString();
    log.info("Final filter {}", finalFilter);
    if (StringUtils.isNotBlank(finalFilter)) {
      return HQLParser.parseExpr(finalFilter);
    }
    return ast;
  }

  public static ASTNode pushBridgeFilter(ASTNode ast, String tableAlias, Set<String> cols, List<ASTNode> pushedFilters)
    throws LensException {
    if (ast == null) {
      return null;
    }
    if (ast.getType() == HiveParser.KW_AND) {
      List<ASTNode> children = Lists.newArrayList();
      for (Node child : ast.getChildren()) {
        ASTNode newChild = pushBridgeFilter((ASTNode) child, tableAlias, cols, pushedFilters);
        if (newChild != null) {
          children.add(newChild);
        }
      }
      if (children.size() == 0) {
        return null;
      } else if (children.size() == 1) {
        return children.get(0);
      } else {
        ASTNode newASTNode = new ASTNode(ast.getToken());
        for (ASTNode child : children) {
          newASTNode.addChild(child);
        }
        return newASTNode;
      }
    }
    if (isPrimitiveBooleanExpression(ast)) {
      if (hasBridgeCol(ast, tableAlias, cols)) {
        pushedFilters.add(ast);
        return null;
      }
    }
    return ast;
  }

  static boolean hasBridgeCol(ASTNode astNode, String tableAlias, Set<String> cols) throws LensException {
    Set<String> bridgeCols = HQLParser.getColsInExpr(tableAlias, cols, astNode);
    return !bridgeCols.isEmpty();
  }
}
