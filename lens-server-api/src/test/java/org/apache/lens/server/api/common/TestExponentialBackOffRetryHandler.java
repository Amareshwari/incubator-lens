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
package org.apache.lens.server.api.common;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class TestExponentialBackOffRetryHandler {

  @Test
  public void testExponentialBackOff() {
    FailureContext failures = new FailureContext();
    ExponentialBackOffRetryHandler retryHandler = new ExponentialBackOffRetryHandler(10, 10000, 1000);
    assertFalse(retryHandler.hasExhaustedRetries(failures));
    assertTrue(retryHandler.canTryNow(failures));

    long now = System.currentTimeMillis();
    failures.updateFailure();
    assertFalse(retryHandler.hasExhaustedRetries(failures));
    assertFalse(retryHandler.canTryNow(failures));
    assertTrue(now + 500 < retryHandler.getNextUpdateTime(failures));
    assertTrue(now + 15000 > retryHandler.getNextUpdateTime(failures));

    for (int i = 0; i < 10; i++) {
      failures.updateFailure();
    }
    assertTrue(retryHandler.hasExhaustedRetries(failures));
    assertFalse(retryHandler.canTryNow(failures));

    failures.clear();
    assertFalse(retryHandler.hasExhaustedRetries(failures));
    assertTrue(retryHandler.canTryNow(failures));
  }
}
