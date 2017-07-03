/*
 *
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
 *
 */

package com.pingcap.tikv.operation;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import static com.pingcap.tikv.operation.BackOff.BackOffType.*;

// TODO this has not finish yet.
public class BackOff {
    static int copBuildTaskMaxBackoff  = 5000;
    static int tsoMaxBackoff           = 5000;
    static int scannerNextMaxBackoff   = 15000;
    static int batchGetMaxBackoff      = 15000;
    static int copNextMaxBackoff       = 15000;
    static int getMaxBackoff           = 15000;
    static int prewriteMaxBackoff      = 15000;
    static int commitMaxBackoff        = 15000;
    static int cleanupMaxBackoff       = 15000;
    static int gcMaxBackoff            = 100000;
    static int gcResolveLockMaxBackoff = 100000;
    static int rawkvMaxBackoff         = 15000;
    private static final Logger logger = LogManager.getFormatterLogger(BackOff.class);
    private final int maxSleep;
    private int totalSleep;
    private int attempts;
    private int lastSleep;

    public enum BackOffType {
        BO_TIKV_RPC,
        BO_TXN_LOCK,
        BO_TXN_LOCK_FAST,
        BO_PD_RPC,
        BO_REGION_MISS,
        BO_SERVER_BUSY
    }

    private BackOff(int maxSleep) {
        this.maxSleep = maxSleep;
    }

    private Map<BackOffType, Supplier<Integer>> fnMap = ImmutableMap.<BackOffType, Supplier<Integer>>builder()
            .put(BO_TIKV_RPC, newBackOffSupplier(100, 2000, JitterType.EQUAL_JITTER))
            .put(BO_TXN_LOCK, newBackOffSupplier(200, 3000, JitterType.EQUAL_JITTER))
            .put(BO_TXN_LOCK_FAST, newBackOffSupplier(100, 3000, JitterType.EQUAL_JITTER))
            .put(BO_PD_RPC, newBackOffSupplier(500, 3000, JitterType.EQUAL_JITTER))
            .put(BO_REGION_MISS, newBackOffSupplier(100, 500, JitterType.NO_JITTER))
            .put(BO_SERVER_BUSY, newBackOffSupplier(2000, 10000, JitterType.EQUAL_JITTER))
            .build();

    private Supplier<Integer> createSleepTimeSupplier(BackOffType type) {
        return fnMap.get(type);
    }

    public enum JitterType {
        NO_JITTER,
        FULL_JITTER,
        EQUAL_JITTER,
        DECORR_JITTER,
    }

    private Supplier<Integer> newBackOffSupplier(int base, int cap, JitterType jitter) {
        int sleep = 0;
        lastSleep = base;
        Random random = new Random();
        return () -> {
            switch (jitter) {
                case NO_JITTER:
                    return expo(base, cap, attempts);
                case FULL_JITTER:
                    {
                        int maxSleep = expo(base, cap, attempts);
                        return random.nextInt(maxSleep);
                    }
                case EQUAL_JITTER:
                {
                    int maxSleep = expo(base, cap, attempts);
                    return maxSleep/2 + random.nextInt(maxSleep/2);
                }
                case DECORR_JITTER:
                {
                    int maxSleep = expo(base, cap, attempts);
                    return Math.min(cap, base + random.nextInt(lastSleep*3 - base));
                }
            }
            try {
                Thread.sleep(sleep);
                attempts++;
                lastSleep = sleep;
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
            return lastSleep;
        };
    }

    private int expo(int base, int cap, int n) {
        return (int) Math.min(cap, base * Math.pow(2.0, n));
    }

    public void backOff(BackOffType type) {
        Supplier<Integer> sleepTime = createSleepTimeSupplier(type);
        this.totalSleep += sleepTime.get();
        if (!canBackOff()) {
            logger.error(String.format("backOff maxSleep %d exceeds backOff totalSleep %d", this.maxSleep, this.totalSleep));
        }
    }

    public boolean canBackOff() {
       return this.maxSleep > 0 && this.totalSleep >= this.maxSleep;
    }
}
