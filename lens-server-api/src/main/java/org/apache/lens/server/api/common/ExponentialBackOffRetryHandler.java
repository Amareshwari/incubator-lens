/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.server.api.common;

public class ExponentialBackOffRetryHandler {

  static final int[] FIBONACCI = new int[] { 1, 1, 2, 3, 5, 8, 13, 21, 34, 45 };

  private long lastFailedTime = 0;
  private int retryAttempt = 0;

  public boolean canTryNow(long maxDelay, long waitSecFactor) {
    if (retryAttempt != 0) {
      long now = System.currentTimeMillis();
      if (now < getNextUpdateTime(maxDelay, waitSecFactor)) {
        return false;
      }
    }
    return true;
  }

  synchronized long getNextUpdateTime(long maxDelay, long waitSecFactor) {
    if (retryAttempt >= FIBONACCI.length) {
      return lastFailedTime + maxDelay;
    }
    long delay = Math.min(maxDelay, FIBONACCI[retryAttempt] * waitSecFactor);
    return lastFailedTime + delay;
  }

  public synchronized boolean hasExhaustedRetries() {
    if (retryAttempt >= FIBONACCI.length) {
      return true;
    }
    return false;
  }

  public synchronized void updateFailure() {
    lastFailedTime = System.currentTimeMillis();
    retryAttempt++;
  }

  public synchronized void clear() {
    lastFailedTime = 0;
    retryAttempt = 0;
  }
}
