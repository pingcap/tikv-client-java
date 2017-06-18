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

package com.pingcap.tikv.types;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.pingcap.tikv.types.Types.*;

/**
 * Create DataType according to Type Flag.
 */
public class DataTypeFactory {
    // TODO add a test for testing mapping relationship
    private static final Map<Integer, DataType> dataTypeMap = ImmutableMap.<Integer, DataType>builder()
            .put(TYPE_NULL, new DataType(TYPE_NULL))
            .put(TYPE_TINY, IntegerType.of(TYPE_TINY))
            .put(TYPE_SHORT, IntegerType.of(TYPE_SHORT))
            .put(TYPE_LONG, IntegerType.of(TYPE_LONG))
            .put(TYPE_INT24, IntegerType.of(TYPE_INT24))
            .put(TYPE_LONG_LONG, IntegerType.of(TYPE_LONG_LONG))
            .put(TYPE_YEAR, IntegerType.of(TYPE_YEAR))
            .put(TYPE_BIT, IntegerType.of(TYPE_BIT))
            .put(TYPE_NEW_DECIMAL, DecimalType.of(TYPE_NEW_DECIMAL))
            .put(TYPE_FLOAT, RealType.of(TYPE_FLOAT))
            .put(TYPE_DOUBLE, RealType.of(TYPE_DOUBLE))
            .put(TYPE_DURATION, TimestampType.of(TYPE_DURATION))
            .put(TYPE_DATETIME, TimestampType.of(TYPE_DATETIME))
            .put(TYPE_TIMESTAMP, TimestampType.of(TYPE_TIMESTAMP))
            .put(TYPE_NEW_DATE, TimestampType.of(TYPE_NEW_DATE))
            .put(TYPE_DATE, TimestampType.of(TYPE_DATE))
            .put(TYPE_VARCHAR, BytesType.of(TYPE_VARCHAR))
            .put(TYPE_JSON, BytesType.of(TYPE_JSON))
            .put(TYPE_ENUM, BytesType.of(TYPE_ENUM))
            .put(TYPE_SET, BytesType.of(TYPE_SET))
            .put(TYPE_TINY_BLOB, BytesType.of(TYPE_TINY_BLOB))
            .put(TYPE_MEDIUM_BLOB, BytesType.of(TYPE_MEDIUM_BLOB))
            .put(TYPE_LONG_BLOB, BytesType.of(TYPE_LONG_BLOB))
            .put(TYPE_BLOB, BytesType.of(TYPE_BLOB))
            .put(TYPE_VAR_STRING, BytesType.of(TYPE_VAR_STRING))
            .put(TYPE_STRING, BytesType.of(TYPE_STRING))
            .put(TYPE_GEOMETRY, BytesType.of(TYPE_GEOMETRY))
            .build();

    public static DataType of(int tp) {
        DataType dataType = dataTypeMap.get(tp);
        if ( dataType == null) {
            throw new NullPointerException("tp " + tp + " passed in can not retrieved DataType info.");
        }
        return dataType;
    }
}
