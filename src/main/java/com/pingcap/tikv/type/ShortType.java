package com.pingcap.tikv.type;


import com.pingcap.tikv.meta.TiColumnInfo;

public class ShortType extends IntegerBaseType {
    public static final int TYPE_CODE = 2;

    public ShortType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
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
