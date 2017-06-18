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

package com.pingcap.tikv.predicates;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.meta.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class ScanBuilderTest {
    @Test
    public void buildScan() throws Exception {
        TiColumnInfo c1 = new TiColumnInfo(
                1,
                CIStr.newCIStr("c1"),
                0,
                TiColumnInfo.DEF_INT_INTERNALTYPE,
                0,
                "");
        TiColumnInfo c2 = new TiColumnInfo(
                2,
                CIStr.newCIStr("c2"),
                0,
                TiColumnInfo.DEF_STR_INTERNALTYPE,
                0,
                "");


        TiIndexInfo index = new TiIndexInfo(
                1,
                CIStr.newCIStr("testIndex"),
                CIStr.newCIStr("test"),
                ImmutableList.of(c1.toIndexColumn(), c2.toIndexColumn()),
                true,
                true,
                SchemaState.StatePublic.getStateCode(),
                "Fake Column",
                IndexType.IndexTypeHash.getTypeCode()
        );

        TiTableInfo table = new TiTableInfo(
                0,
                CIStr.newCIStr("test"),
                "",
                "",
                false,
                ImmutableList.of(c1, c2),
                ImmutableList.of(index),
                "",
                0,
                0,
                0,
                0
        );

        ScanBuilder builder = new ScanBuilder();
        List<TiExpr> exprs = ImmutableList.of(
                new Equal(TiColumnRef.create("c1", table), TiConstant.create(0)),
                new Equal(TiColumnRef.create("c2", table), TiConstant.create("test"))
        );
        builder.buildScan(exprs, index, table);
    }

}