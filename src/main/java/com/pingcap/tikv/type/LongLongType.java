package com.pingcap.tikv.type;


import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class LongLongType extends IntegerBaseType<Long> {
    public static final int TYPE_CODE = 8;

    public LongLongType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected LongLongType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Long value) {
        row.setLong(pos, value);
    }

    @Override
    public Long decodeNotNull(int flag, CodecDataInput cdi) {
        return decodeNotNullInternal(flag, cdi);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static LongLongType DEF_SIGNED_TYPE = new LongLongType(false);
    public final static LongLongType DEF_UNSIGNED_TYPE = new LongLongType(true);
}
