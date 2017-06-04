package com.pingcap.tikv.type;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;

public class BooleanType extends FieldType {
    public static BooleanType DEF_BOOLEAN_TYPE = new BooleanType();
    @Override
    protected void decodeValueNoNullToRow(CodecDataInput cdi, Row row, int pos) {

    }

    @Override
    protected boolean isValidFlag(int flag) {
        return false;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public int getTypeCode() {
        return 0;
    }
}
