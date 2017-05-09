package com.pingcap.tikv.codec;

/**
 * Created by zhexuany on 5/3/17.
 */
public class Datum {
    byte kind;
    long collaton;
    long decimal;
    long length;
    int i;
    byte[] b;
    Object x;
}
