package com.pingcap.tikv.types;


import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class TinyType extends IntegerBaseType<Short> {
    public static final int TYPE_CODE = 1;
    public TinyType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Short value) {
        row.setInteger(pos, value);
    }

    @Override
    public Short decodeNotNull(int flag, CodecDataInput cdi) {
        return (short)decodeNotNullInternal(flag, cdi);
    }

    protected TinyType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static TinyType DEF_SIGNED_TYPE = new TinyType(false);
    public final static TinyType DEF_UNSIGNED_TYPE = new TinyType(true);
}