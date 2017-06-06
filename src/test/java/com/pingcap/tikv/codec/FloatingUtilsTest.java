package com.pingcap.tikv.codec;


import org.junit.Test;

import static org.junit.Assert.*;



public class FloatingUtilsTest {
    @Test
    public void readFloatTest() throws Exception {
        byte [] data = new byte[] {(byte)(191 & 0xFF),
                                   (byte)(241 & 0xFF),
                                   (byte)(153 & 0xFF),
                                   (byte)(153 & 0xFF),
                                   (byte)(160 & 0xFF),
                                    0,0,0};
        CodecDataInput cdi = new CodecDataInput(data);
        double u = FloatingUtils.readDouble(cdi);
        assertEquals(1.1, u, 0.0001);

        data = new byte[] {(byte)(192 & 0xFF),
                (byte)(1 & 0xFF),
                (byte)(153 & 0xFF),
                (byte)(153 & 0xFF),
                (byte)(153 & 0xFF),
                (byte)(153 & 0xFF),
                (byte)(153 & 0xFF),
                (byte)(154 & 0xFF)};
        cdi = new CodecDataInput(data);
        u = FloatingUtils.readDouble(cdi);
        assertEquals(2.2, u, 0.0001);

        data = new byte[]
                {(byte)(63 & 0xFF),
                (byte)(167 & 0xFF),
                (byte)(51 & 0xFF),
                (byte)(67 & 0xFF),
                (byte)(159 & 0xFF),
                (byte)(255 & 0xFF),
                (byte)(255 & 0xFF),
                (byte)(255 & 0xFF)};

        cdi = new CodecDataInput(data);
        u = FloatingUtils.readDouble(cdi);
        assertEquals(-99.199, u, 0.0001);
    }
}