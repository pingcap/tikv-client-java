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
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.types.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_STRING;
import static org.junit.Assert.*;


public class ScanBuilderTest {
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

    private static TiTableInfo createTableWithPrefix() {
        DataType typePrefx = BytesType.of(TYPE_STRING);
        return new MetaUtils.TableBuilder()
                .name("testTable")
                .addColumn("c1", DataTypeFactory.of(Types.TYPE_LONG), true)
                .addColumn("c2", DataTypeFactory.of(Types.TYPE_STRING))
                .addColumn("c3", DataTypeFactory.of(Types.TYPE_STRING))
                .addColumn("c4", DataTypeFactory.of(Types.TYPE_TINY))
                .appendIndex("testIndex", ImmutableList.of("c1", "c2", "c3"), false)
                .build();
    }

    @Test
    public void extractConditions() throws Exception {
        TiTableInfo table = createTable();
        TiIndexInfo index = table.getIndices().get(0);

        TiExpr eq1 = new Equal(TiColumnRef.create("c1", table), TiConstant.create(0));
        TiExpr eq2 = new Equal(TiColumnRef.create("c2", table), TiConstant.create("test"));
        TiExpr le1 = new LessEqual(TiColumnRef.create("c3", table), TiConstant.create("fxxx"));
        // Last one should be pushed back
        TiExpr eq3 = new Equal(TiColumnRef.create("c4", table), TiConstant.create("fxxx"));

        List<TiExpr> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

        ScanBuilder.IndexMatchingResult result = ScanBuilder.extractConditions(exprs, table, index);
        assertEquals(1, result.residualConditions.size());
        assertEquals(eq3, result.residualConditions.get(0));

        assertEquals(2, result.accessPoints.size());
        assertEquals(eq1, result.accessPoints.get(0));
        assertEquals(eq2, result.accessPoints.get(1));

        assertEquals(1, result.accessConditions.size());
        assertEquals(le1, result.accessConditions.get(0));
        return;
    }

    @Test
    public void extractConditionsWithPrefix() throws Exception {
        TiTableInfo table = createTable();
        TiIndexInfo index = table.getIndices().get(0);

        TiExpr eq1 = new Equal(TiColumnRef.create("c1", table), TiConstant.create(0));
        TiExpr eq2 = new Equal(TiColumnRef.create("c2", table), TiConstant.create("test"));
        TiExpr le1 = new LessEqual(TiColumnRef.create("c3", table), TiConstant.create("fxxx"));
        // Last one should be pushed back
        TiExpr eq3 = new Equal(TiColumnRef.create("c4", table), TiConstant.create("fxxx"));

        List<TiExpr> exprs = ImmutableList.of(eq1, eq2, le1, eq3);

        ScanBuilder.IndexMatchingResult result = ScanBuilder.extractConditions(exprs, table, index);
        assertEquals(1, result.residualConditions.size());
        assertEquals(eq3, result.residualConditions.get(0));

        assertEquals(2, result.accessPoints.size());
        assertEquals(eq1, result.accessPoints.get(0));
        assertEquals(eq2, result.accessPoints.get(1));

        assertEquals(1, result.accessConditions.size());
        assertEquals(le1, result.accessConditions.get(0));
        return;
    }

}