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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DecimalType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DecimalTypeTest {
    @Test
    public void writeDoubleAndReadDoubleTest() {
        CodecDataOutput cdo = new CodecDataOutput();
        DecimalType.writeDouble(cdo, 0.00);
        double u = DecimalType.readDouble(new CodecDataInput(cdo.toBytes()));
        assertEquals(0.00, u, 0.01);

        cdo.reset();
        DecimalType.writeDouble(cdo, 206.0);
        u = DecimalType.readDouble(new CodecDataInput(cdo.toBytes()));
        assertEquals(206.0, u, 0.0001);

        cdo.reset();
        DecimalType.writeDouble(cdo, Double.MIN_VALUE);
        u = DecimalType.readDouble(new CodecDataInput(cdo.toBytes()));
        assertEquals(Double.MIN_VALUE, u, 0);
    }
}
