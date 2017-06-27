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

package com.pingcap.tikv.operation.transformer;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;

import java.util.List;

public class Skip implements Projection {
    public static final Skip SKIP_OP = new Skip();

    @Override
    public void append(Object value, Row row) {}

    @Override
    public int size() {
        return 0;
    }

    @Override
    public List<DataType> getTypes() {
        return ImmutableList.of();
    }
}
