/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.policy;

import com.google.common.base.Preconditions;
import com.pingcap.tikv.operation.ErrorHandler;

public class RetryNTimes<T> extends RetryPolicy<T> {
  private int[] fibonacci;

  private RetryNTimes(int n, ErrorHandler<T> handler) {
    super(handler);
    Preconditions.checkArgument(n >= 1, "Retry count cannot be less than 1.");
    this.fibonacci = generateFibonacci(n);
  }

  @Override
  public int[] getFibonacci() {
    return fibonacci;
  }

  private int[] generateFibonacci(int n) {
    int[] fib = new int[n+1];
    fib[0] = 0;
    fib[1] = 1;
    for(int i = 2; i <= n; i++) {
      fib[i] = fib[i-1] + fib[i-2];
    }
    return fib;
  }

  public static Builder newBuilder(int n) {
    return new Builder(n);
  }

  public static class Builder<T> implements RetryPolicy.Builder<T> {
    private int n;

    public Builder(int n) {
      this.n = n;
    }

    @Override
    public RetryPolicy<T> create(ErrorHandler<T> handler) {
      return new RetryNTimes<>(n, handler);
    }
  }
}
