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

package com.pingcap.tikv.expression;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTableInfoTest;
import com.pingcap.tikv.types.RealType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TiConstantTest {
  @Test
  public void greaterThanTest() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    TiTableInfo tableInfo = mapper.readValue(TiTableInfoTest.tableJson, TiTableInfo.class);
    GreaterThan g = new GreaterThan(TiColumnRef.create("c1", tableInfo), TiConstant.create(1.12));
    Expr ge = g.toProto();
    assertEquals(2, ge.getChildrenCount());
    double expected = RealType.readDouble(new CodecDataInput(ge.getChildren(1).getVal()));
    assertEquals(1.12, expected, 0.00001);
  }

//  @Test
//  public void testEncodeSQLDate() {
//    Calendar calendar = Calendar.getInstance();
//    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
//    calendar.set(1998, Calendar.SEPTEMBER, 2);
//
//    TiConstant sqlDate = TiConstant.create(new java.sql.Date(calendar.getTime().getTime()));
//
//    assertEquals("tp: MysqlTime\nval: \"\\031_\\304\\000\\000\\000\\000\\000\"\n",
//        sqlDate.toProto().toString());
//  }

//  @Test
//  public void testEncodeTimestamp() {
//    TiConstant tsDate = TiConstant.create(new Timestamp(904741201002L));
//    assertEquals("tp: MysqlTime\nval: \"\\031_\\305P\\001\\000\\a\\320\"\n",
//        tsDate.toProto().toString());
//  }
}
