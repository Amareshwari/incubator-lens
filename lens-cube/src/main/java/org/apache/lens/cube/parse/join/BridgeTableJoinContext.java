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


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.lens.cube.metadata.join.TableRelationship;
import org.apache.lens.cube.parse.*;

/**
 * Join context related to Bridge tables
 */
@Slf4j
public class BridgeTableJoinContext {
  private final String bridgeTableFieldAggr;
  private final CubeQueryContext cubeql;
  private boolean initedBridgeClauses = false;
  private final StringBuilder bridgeSelectClause = new StringBuilder();
  private final StringBuilder bridgeFromClause = new StringBuilder();
  private final StringBuilder bridgeFilterClause = new StringBuilder();
  private final StringBuilder bridgeJoinClause = new StringBuilder();
  private final StringBuilder bridgeGroupbyClause = new StringBuilder();

  public BridgeTableJoinContext(CubeQueryContext cubeql, String bridgeTableFieldAggr) {
    this.bridgeTableFieldAggr = bridgeTableFieldAggr;
    this.cubeql = cubeql;
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
  public void updateBridgeClause(TableRelationship rel, String fromAlias, String toAlias, CandidateDim toTable, String
    userFilter, String storageFilter) {
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

  public String generateJoinClause(String joinTypeStr, String toAlias) {
    StringBuilder clause = new StringBuilder(joinTypeStr);
    clause.append(" join ");
    clause.append(bridgeSelectClause.toString());
    // iterate over all select expressions and add them for select clause if do_flattening_early is disabled
    // TODO
    for (String col : cubeql.getTblAliasToColumns().get(toAlias)) {
      clause.append(",").append(bridgeTableFieldAggr).append("(").append(toAlias)
        .append(".").append(col)
        .append(")")
        .append(" as ").append(col);
    }
    String bridgeFrom = bridgeFromClause.toString();
    clause.append(bridgeFrom);
    String bridgeFilter = bridgeFilterClause.toString();
    if (StringUtils.isNotBlank(bridgeFilter)) {
      if (bridgeFrom.contains(" join ")) {
        clause.append(" and ");
      } else {
        clause.append(" where");
      }
      clause.append(bridgeFilter);
    }
    // Add filters from main query here, if do_flattening_early is disabled
    // TODO
    clause.append(bridgeGroupbyClause.toString());
    clause.append(") ").append(toAlias);
    clause.append(String.format(bridgeJoinClause.toString(), toAlias));
    return clause.toString();
  }
}
