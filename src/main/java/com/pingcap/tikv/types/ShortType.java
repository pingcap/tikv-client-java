package com.pingcap.tikv.types;


import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class ShortType extends IntegerBaseType<Short> {
    public static final int TYPE_CODE = 2;

    public ShortType(TiColumnInfo.InternalTypeHolder holder) {
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

    protected ShortType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static ShortType DEF_SIGNED_TYPE = new ShortType(false);
    public final static ShortType DEF_UNSIGNED_TYPE = new ShortType(true);
}