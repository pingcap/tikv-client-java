package com.pingcap.tikv.type;


import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.Row;
import com.pingcap.tikv.meta.TiColumnInfo;

public class LongType extends IntegerBaseType<Long> {
    public static final int TYPE_CODE = 3;

    public LongType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    @Override
    protected void decodeValueNoNullToRow(Row row, int pos, Long value) {
        row.setLong(pos, value);
    }

    @Override
    public Long decodeNotNull(int flag, CodecDataInput cdi) {
        return decodeNotNullInternal(flag, cdi);
    }

    protected LongType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static LongType DEF_SIGNED_TYPE = new LongType(false);
    public final static LongType DEF_UNSIGNED_TYPE = new LongType(true);
}
