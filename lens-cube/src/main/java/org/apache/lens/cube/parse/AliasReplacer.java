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
package org.apache.lens.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_SELEXPR;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.Dimension;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.antlr.runtime.CommonToken;

import lombok.extern.slf4j.Slf4j;

/**
 * Finds queried column to table alias. Finds queried dim attributes and queried measures.
 * <p/>
 * Does queried field validation wrt derived cubes, if all fields of queried cube cannot be queried together.
 * <p/>
 * Replaces all the columns in all expressions with tablealias.column
 */
@Slf4j
class AliasReplacer implements ContextRewriter {

  public AliasReplacer(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    Map<String, String> colToTableAlias = cubeql.getColToTableAlias();

    extractTabAliasForCol(cubeql);
    findDimAttributesAndMeasures(cubeql);

    if (colToTableAlias.isEmpty()) {
      return;
    }

    // Rewrite the all the columns in the query with table alias prefixed.
    // If col1 of table tab1 is accessed, it would be changed as tab1.col1.
    // If tab1 is already aliased say with t1, col1 is changed as t1.col1
    // replace the columns in select, groupby, having, orderby by
    // prepending the table alias to the col
    // sample select trees
    // 1: (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))
    // (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL value))))
    // 2: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key))
    // (TOK_SELEXPR (TOK_FUNCTION count (. (TOK_TABLE_OR_COL src) value))))
    // 3: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) srckey))))
    replaceAliases(cubeql.getSelectAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getHavingAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getOrderByAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getGroupByAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getWhereAST(), 0, colToTableAlias);

    replaceAliases(cubeql.getJoinTree(), 0, colToTableAlias);

    // Update the aggregate expression set
    AggregateResolver.updateAggregates(cubeql.getSelectAST(), cubeql);
    AggregateResolver.updateAggregates(cubeql.getHavingAST(), cubeql);
    // Update alias map as well
    updateAliasMap(cubeql.getSelectAST(), cubeql);
  }

  /**
   * Figure out queried dim attributes and measures from the cube query context
   * @param cubeql
   * @throws SemanticException
   */
  private void findDimAttributesAndMeasures(CubeQueryContext cubeql) throws SemanticException {
    CubeInterface cube = cubeql.getCube();
    if (cube != null) {
      Set<String> cubeColsQueried = cubeql.getColumnsQueried(cube.getName());
      Set<String> queriedDimAttrs = new HashSet<String>();
      Set<String> queriedMsrs = new HashSet<String>();
      Set<String> queriedExprs = new HashSet<String>();
      if (cubeColsQueried != null && !cubeColsQueried.isEmpty()) {
        for (String col : cubeColsQueried) {
          if (cube.getMeasureNames().contains(col)) {
            queriedMsrs.add(col);
          } else if (cube.getDimAttributeNames().contains(col)) {
            queriedDimAttrs.add(col);
          } else if (cube.getExpressionNames().contains(col)) {
            queriedExprs.add(col);
          }
        }
      }
      cubeql.addQueriedDimAttrs(queriedDimAttrs);
      cubeql.addQueriedMsrs(queriedMsrs);
      cubeql.addQueriedExprs(queriedExprs);
    }
  }

  private void extractTabAliasForCol(CubeQueryContext cubeql) throws SemanticException {
    extractTabAliasForCol(cubeql, cubeql);
  }

  static void extractTabAliasForCol(CubeQueryContext cubeql, TrackQueriedColumns tqc) throws SemanticException {
    Map<String, String> colToTableAlias = cubeql.getColToTableAlias();
    Set<String> columns = tqc.getTblAliasToColumns().get(CubeQueryContext.DEFAULT_TABLE);
    if (columns == null) {
      return;
    }
    for (String col : columns) {
      boolean inCube = false;
      if (cubeql.getCube() != null) {
        Set<String> cols = cubeql.getCube().getAllFieldNames();
        if (cols.contains(col.toLowerCase())) {
          String cubeAlias = cubeql.getAliasForTableName(cubeql.getCube().getName());
          colToTableAlias.put(col.toLowerCase(), cubeAlias);
          tqc.addColumnsQueried(cubeAlias, col.toLowerCase());
          inCube = true;
        }
      }
      for (Dimension dim : cubeql.getDimensions()) {
        if (dim.getAllFieldNames().contains(col.toLowerCase())) {
          if (!inCube) {
            String prevDim = colToTableAlias.get(col.toLowerCase());
            if (prevDim != null && !prevDim.equals(dim.getName())) {
              throw new SemanticException(ErrorMsg.AMBIGOUS_DIM_COLUMN, col, prevDim, dim.getName());
            }
            String dimAlias = cubeql.getAliasForTableName(dim.getName());
            colToTableAlias.put(col.toLowerCase(), dimAlias);
            tqc.addColumnsQueried(dimAlias, col.toLowerCase());
          } else {
            // throw error because column is in both cube and dimension table
            throw new SemanticException(ErrorMsg.AMBIGOUS_CUBE_COLUMN, col, cubeql.getCube().getName(), dim.getName());
          }
        }
      }
      if (colToTableAlias.get(col.toLowerCase()) == null) {
        throw new SemanticException(ErrorMsg.COLUMN_NOT_FOUND, col);
      }
    }
  }

  static void replaceAliases(ASTNode node, int nodePos, Map<String, String> colToTableAlias) {
    if (node == null) {
      return;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String colName = HQLParser.getColName(node);
      String newAlias = colToTableAlias.get(colName.toLowerCase());

      log.info("colName:" + colName + " newAlias:" + newAlias); 
      if (StringUtils.isBlank(newAlias)) {
        return;
      }

      if (nodeType == HiveParser.DOT) {
        // No need to create a new node, just replace the table name ident
        ASTNode aliasNode = (ASTNode) node.getChild(0);
        ASTNode newAliasIdent = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        aliasNode.setChild(0, newAliasIdent);
        newAliasIdent.setParent(aliasNode);
      } else {
        // Just a column ref, we need to make it alias.col
        // '.' will become the parent node
        ASTNode aliasIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        ASTNode tabRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));

        tabRefNode.addChild(aliasIdentNode);
        aliasIdentNode.setParent(tabRefNode);
        ASTNode colIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, colName));
        ASTNode parent = (ASTNode) node.getParent();
        if (parent != null) {
          ASTNode dot = new ASTNode(new CommonToken(HiveParser.DOT, "."));
          dot.addChild(tabRefNode);
          tabRefNode.setParent(dot);
          dot.addChild(colIdentNode);
          parent.setChild(nodePos, dot);
        } else {
          node.getToken().setType(HiveParser.DOT);
          node.getToken().setText(".");
          node.setChild(0, tabRefNode);
          tabRefNode.setParent(node);
          node.addChild(colIdentNode);
        }
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceAliases(child, i, colToTableAlias);
      }
    }
  }

  static void updateAliasMap(ASTNode root, CubeQueryContext cubeql) {
    if (root == null) {
      return;
    }

    if (root.getToken().getType() == TOK_SELEXPR) {
      ASTNode alias = HQLParser.findNodeByPath(root, Identifier);
      if (alias != null) {
        cubeql.addExprToAlias(root, alias);
      }
    } else {
      for (int i = 0; i < root.getChildCount(); i++) {
        updateAliasMap((ASTNode) root.getChild(i), cubeql);
      }
    }
  }

}
