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
import com.google.common.collect.Lists;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.In;
import com.pingcap.tikv.meta.MetaUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;


public class RangeBuilderTest {
    private static TiTableInfo createTable() {
        return new MetaUtils.TableBuilder()
                .name("testTable")
                .addColumn("c1", DataTypeFactory.of(Types.TYPE_LONG), true)
                .addColumn("c2", DataTypeFactory.of(Types.TYPE_STRING))
                .addColumn("c3", DataTypeFactory.of(Types.TYPE_STRING))
                .addColumn("c4", DataTypeFactory.of(Types.TYPE_TINY))
                .appendIndex("testIndex", ImmutableList.of("c1", "c2", "c3"), false)
                .build();
    }

    private static boolean testPointIndexRanges(List<RangeBuilder.IndexRange> ranges,
                                           List<List<Object>> values) {
        if (ranges.size() != values.size()) return false;

        for (RangeBuilder.IndexRange ir : ranges) {
            boolean found = false;
            List<Object> aps = ir.getAccessPoints();
            for (int i = 0; i < values.size(); i++) {
                List<Object> curVals = values.get(i);
                if (curVals.equals(aps)) {
                    values.remove(i);
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }

        return values.isEmpty();
    }

    @Test
    public void exprsToPoints() throws Exception {
        TiTableInfo table = createTable();
        List<TiExpr> conds = ImmutableList.of(
                new Equal(TiColumnRef.create("c1", table), TiConstant.create(0)),
                new Equal(TiConstant.create("v1"), TiColumnRef.create("c2", table))
        );
        List<DataType> types = ImmutableList.of(
                DataTypeFactory.of(Types.TYPE_LONG),
                DataTypeFactory.of(Types.TYPE_STRING)
        );
        RangeBuilder builder = new RangeBuilder();
        List<RangeBuilder.IndexRange> indexRanges = builder.exprsToPoints(conds, types);
        assertEquals(1, indexRanges.size());
        List<Object> acpts = indexRanges.get(0).getAccessPoints();
        assertEquals(2, acpts.size());
        assertEquals(0, acpts.get(0));
        assertEquals("v1", acpts.get(1));

        // In Expr
        conds = ImmutableList.of(
                new In(TiColumnRef.create("c1", table),
                        TiConstant.create(0),
                        TiConstant.create(1),
                        TiConstant.create(3)),
                new Equal(TiConstant.create("v1"), TiColumnRef.create("c2", table)),
                new In(TiColumnRef.create("c3", table),
                                TiConstant.create("2"),
                                TiConstant.create("4"))
        );
        types = ImmutableList.of(
                DataTypeFactory.of(Types.TYPE_LONG),
                DataTypeFactory.of(Types.TYPE_STRING),
                DataTypeFactory.of(Types.TYPE_STRING)
        );

        indexRanges = builder.exprsToPoints(conds, types);
        assertEquals(6, indexRanges.size());
        assertTrue(testPointIndexRanges(indexRanges,
                Lists.newArrayList(
                        ImmutableList.of(0, "v1", "2"),
                        ImmutableList.of(0, "v1", "4"),
                        ImmutableList.of(1, "v1", "2"),
                        ImmutableList.of(1, "v1", "4"),
                        ImmutableList.of(3, "v1", "2"),
                        ImmutableList.of(3, "v1", "4")
                )
        ));
    }

    @Test
    public void exprToRanges() throws Exception {

    }

}