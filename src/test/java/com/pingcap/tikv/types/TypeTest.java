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

package com.pingcap.tikv.types;

import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.RowMeta;
import com.pingcap.tidb.tipb.Select;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.*;
import com.pingcap.tikv.operation.SelectIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TypeTest {
    private final String value = "foo";

    // TODO finish default TiColumnInfo creater.
    // 1. add defeault internalholder creater
    // 2. add int string TiColumnInfo creater;
    public TiTableInfo generateTiTableInfo(String name) {
        List<TiColumnInfo> columnInfos = new ArrayList<>();
        TiColumnInfo c1 = new TiColumnInfo(1, new CIStr("c1", "c1"), 0, TiColumnInfo.DEF_INT_INTERNALTYPE, 0, "");
        TiColumnInfo s1 = new TiColumnInfo(2, new CIStr("s1", "s1"), 0, TiColumnInfo.DEF_STR_INTERNALTYPE, 0, "");
        columnInfos.add(c1);
        columnInfos.add(s1);
        return new TiTableInfo(1, new CIStr(name, name), "", "",
               false, columnInfos, null, "", 2, 2, 1, 1);
    }
    @Test
    public void testLongAndStringType() throws Exception {
        String rowsData = "\b\002\002\006foo";
        RowMeta rowMeta = RowMeta.newBuilder()
                            .setHandle((long)1)
                            .setLength((long)7)
                            .build();
        Chunk chunk = Chunk.newBuilder().addRowsMeta(rowMeta).setRowsData(ByteString.copyFromUtf8(rowsData)).build();
        List<Chunk> chunks = new ArrayList<>();
        chunks.add(chunk);
        TiTableInfo tiTableInfo = generateTiTableInfo("t1");
        TiExpr s1 = TiColumnRef.create("s1", tiTableInfo);
        TiExpr c1 = TiColumnRef.create("c1", tiTableInfo);
        TiSelectRequest req = new TiSelectRequest(SelectRequest.newBuilder());
        req.setTableInfo(tiTableInfo);
        req.getFields().add(c1);
        req.getFields().add(s1);

        SelectIterator it = new SelectIterator(chunks, req);
        Row r  = it.next();
        long val1 = r.getLong(0);
        String val2 = r.getString(1);
        Assert.assertSame("value should be 1", (long)1, val1);
        Assert.assertTrue(value.equals(val2));
    }
}
