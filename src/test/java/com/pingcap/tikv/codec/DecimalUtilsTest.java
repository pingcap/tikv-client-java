package com.pingcap.tikv.codec;

import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.*;
public class DecimalUtilsTest {
    @Test
    public void readDecimalFullyTest() throws Exception {
        CodecDataOutput cdo = new CodecDataOutput();
        DecimalUtils.writeDecimalFully(cdo, 206.0);
        CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
        double value = DecimalUtils.readDecimalFully(cdi);
        assertEquals(206.0, value, 0.0001);
    }
}
