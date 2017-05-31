package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class BooleanType extends IntegerBaseType<Short> {
    public static final int TYPE_CODE = 2;

    public BooleanType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Short value) {
        row.setShort(pos, value);
    }

    @Override
    public Short decodeNotNull(int flag, CodecDataInput cdi) {
        return (short)decodeNotNullInternal(flag, cdi);
    }

    protected BooleanType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static BooleanType DEF_BOOLEAN_TYPE = new BooleanType(true);
}
