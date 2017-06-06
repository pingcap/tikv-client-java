package com.pingcap.tikv.type;


import com.pingcap.tikv.meta.TiColumnInfo;

public class LongType extends IntegerBaseType {
    public static final int TYPE_CODE = 3;

    public LongType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
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
