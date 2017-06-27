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
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;

import java.util.Map;
import java.util.function.Function;

import static com.pingcap.tikv.types.Types.*;

/**
 * Create DataType according to Type Flag.
 */
public class DataTypeFactory {
    // TODO: the type system still needs another overhaul
    private static final Map<Integer, DataType> dataTypeMap = ImmutableMap.<Integer, DataType>builder()
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
            .put(TYPE_TINY_BLOB, RawBytesType.of(TYPE_TINY_BLOB))
            .put(TYPE_MEDIUM_BLOB, RawBytesType.of(TYPE_MEDIUM_BLOB))
            .put(TYPE_LONG_BLOB, RawBytesType.of(TYPE_LONG_BLOB))
            .put(TYPE_BLOB, RawBytesType.of(TYPE_BLOB))
            .put(TYPE_VAR_STRING, BytesType.of(TYPE_VAR_STRING))
            .put(TYPE_STRING, BytesType.of(TYPE_STRING))
            .put(TYPE_GEOMETRY, BytesType.of(TYPE_GEOMETRY))
            .build();

    private static final Map<Integer, Function<InternalTypeHolder, DataType>>
            dataTypeCreatorMap = ImmutableMap.<Integer, Function<InternalTypeHolder, DataType>>builder()
                .put(TYPE_TINY, h -> new IntegerType(h))
                .put(TYPE_SHORT, h -> new IntegerType(h))
                .put(TYPE_LONG, h -> new IntegerType(h))
                .put(TYPE_INT24, h -> new IntegerType(h))
                .put(TYPE_LONG_LONG, h -> new IntegerType(h))
                .put(TYPE_YEAR, h -> new IntegerType(h))
                .put(TYPE_BIT, h -> new IntegerType(h))
                .put(TYPE_NEW_DECIMAL, h -> new DecimalType(h))
                .put(TYPE_FLOAT, h -> new RealType(h))
                .put(TYPE_DOUBLE, h -> new RealType(h))
                .put(TYPE_DURATION, h -> new TimestampType(h))
                .put(TYPE_DATETIME, h -> new TimestampType(h))
                .put(TYPE_TIMESTAMP, h -> new TimestampType(h))
                .put(TYPE_NEW_DATE, h -> new TimestampType(h))
                .put(TYPE_DATE, h -> new TimestampType(h))
                .put(TYPE_VARCHAR, h -> new BytesType(h))
                .put(TYPE_JSON, h -> new BytesType(h))
                .put(TYPE_ENUM, h -> new BytesType(h))
                .put(TYPE_SET, h -> new BytesType(h))
                .put(TYPE_TINY_BLOB, h -> new RawBytesType(h))
                .put(TYPE_MEDIUM_BLOB, h -> new RawBytesType(h))
                .put(TYPE_LONG_BLOB, h -> new RawBytesType(h))
                .put(TYPE_BLOB, h -> new RawBytesType(h))
                .put(TYPE_VAR_STRING, h -> new BytesType(h))
                .put(TYPE_STRING, h -> new BytesType(h))
                .put(TYPE_GEOMETRY, h -> new BytesType(h))
                .build();

    public static DataType of(int tp) {
        DataType dataType = dataTypeMap.get(tp);
        if ( dataType == null) {
            throw new NullPointerException("tp " + tp + " passed in can not retrieved DataType info.");
        }
        return dataType;
    }

    public static DataType of(InternalTypeHolder holder) {
        Function<InternalTypeHolder, DataType> ctor = dataTypeCreatorMap.get(holder.getTp());
        if (ctor == null) {
            throw new NullPointerException("tp " + holder.getTp() + " passed in can not retrieved DataType info.");
        }
        return ctor.apply(holder);
    }
}
