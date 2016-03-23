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
    ExponentialBackOffRetryHandler retryHandler = new ExponentialBackOffRetryHandler();
    assertFalse(retryHandler.hasExhaustedRetries());
    assertTrue(retryHandler.canTryNow(10000, 1000));

    long now = System.currentTimeMillis();
    retryHandler.updateFailure();
    assertFalse(retryHandler.hasExhaustedRetries());
    assertFalse(retryHandler.canTryNow(10000, 1000));
    assertTrue(now + 500 < retryHandler.getNextUpdateTime(10000, 1000));
    assertTrue(now + 15000 > retryHandler.getNextUpdateTime(10000, 20000));

    for (int i = 0; i < ExponentialBackOffRetryHandler.FIBONACCI.length; i++) {
      retryHandler.updateFailure();
    }
    assertTrue(retryHandler.hasExhaustedRetries());
    assertFalse(retryHandler.canTryNow(10000, 1000));

    retryHandler.clear();
    assertFalse(retryHandler.hasExhaustedRetries());
    assertTrue(retryHandler.canTryNow(10000, 1000));
  }
}
