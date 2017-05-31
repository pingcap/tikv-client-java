package com.pingcap.tikv.codec;


public class FloatUtils {
    /**
     * Encoding a float value to byte buffer
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeFloat(CodecDataOutput cdo, float val) {
        throw new UnsupportedOperationException();
    }

    /**
     * Encoding a double value to byte buffer
     * @param cdo For outputting data in bytes array
     * @param val The data to encode
     */
    public static void writeDouble(CodecDataOutput cdo, double val) {
        throw new UnsupportedOperationException();
    }
}
