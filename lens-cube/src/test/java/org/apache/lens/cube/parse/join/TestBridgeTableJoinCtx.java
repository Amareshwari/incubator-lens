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

import static org.testng.Assert.assertEquals;

import org.apache.lens.cube.parse.HQLParser;
import org.apache.lens.server.api.error.LensException;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestBridgeTableJoinCtx {


  @DataProvider(name = "filterReplace")
  public Object[][] mediaTypeData() {
    return new Object[][] {
      {"t1.c1 in ('XyZ', 'abc', 'PQR', 'lKg')", "myfilter(( t1 . c1 ), 'XyZ' ) or myfilter(( t1 . c1 ), 'abc' )"
        + " or myfilter(( t1 . c1 ), 'PQR' ) or myfilter(( t1 . c1 ), 'lKg' )"},
      {"t1.c1 = ('XyZ')", "myfilter(( t1 . c1 ), 'XyZ' )"},
      {"t1.c1 != ('XyZ')", "not myfilter(( t1 . c1 ), 'XyZ' )"},
    };
  }

  @Test(dataProvider = "filterReplace")
  public void testReplaceDirectFiltersWithArrayFilter(String filter, String expected) throws LensException {
    ASTNode replaced = BridgeTableJoinContext.replaceDirectFiltersWithArrayFilter(HQLParser.parseExpr(filter),
      "myfilter");
    String replacedFilter = HQLParser.getString(replaced);
    assertEquals(replacedFilter, expected);
  }
}
