package com.pingcap.tikv.codec;

import org.junit.Test;
import java.util.Arrays;
import com.pingcap.tikv.codec.MyDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
public class MyDecimalTest {
    @Test
    public void fromStringTest() throws Exception {
        List<MyDecimalTestStruct> test = new ArrayList<>();
        test.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
        test.add(new MyDecimalTestStruct("123.45", "123.45", 5, 2));
        test.add(new MyDecimalTestStruct("-123.45", "-123.45", 5, 2));
        test.add(new MyDecimalTestStruct(".00012345000098765", "0.00012345000098765", 17, 17));
        test.add(new MyDecimalTestStruct(".12345000098765", "0.12345000098765", 14, 14));
        test.add(new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 21, 21));
        test.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
        test.add(new MyDecimalTestStruct("1234500009876.5", "1234500009876.5", 14, 1));
        test.forEach((a) ->
                     {
                         MyDecimal dec = new MyDecimal();
                         dec.fromString(a.in);
                         assertEquals(a.precision, dec.precision());
                         assertEquals(a.frac, dec.frac());
                         assertEquals(a.out, dec.toString());
                     }
                     );
    }

    @Test
    public void toBinToBinFromBinTest() throws Exception {
        List<MyDecimalTestStruct> test = new ArrayList<>();
        test.add(new MyDecimalTestStruct("-10.55", "-10.55", 4, 2));
        test.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
        test.add(new MyDecimalTestStruct("-12345", "-12345", 5, 0));
        test.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
        test.add(new MyDecimalTestStruct("0.00012345000098765", "0.00012345000098765", 17, 17));
        test.add(new MyDecimalTestStruct("-0.00012345000098765", "-0.00012345000098765", 17, 17));
        test.forEach((a) ->
                     {
                         MyDecimal dec = new MyDecimal();
                         dec.fromString(a.in);
                         assertEquals(a.out, dec.toString());
                         int[] bin = dec.toBin(dec.precision(), dec.frac());
                         dec.clear();
                         dec.fromBin(a.precision, a.frac, bin);
                         assertEquals(a.precision, dec.precision());
                         assertEquals(a.frac, dec.frac());
                         assertEquals(a.out, dec.toString());
                     }
                     );
    }

    // MyDecimalTestStruct is only used for simplifing testing.
    private class MyDecimalTestStruct {
        String in;
        String out;
        int precision;
        int frac;
        MyDecimalTestStruct(String in, String out, int precision, int frac) {
            this.in = in;
            this.out = out;
            this.precision = precision;
            this.frac = frac;
        }
    }
}
