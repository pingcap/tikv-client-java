package com.pingcap.tikv.type;


import com.pingcap.tikv.meta.TiColumnInfo;

public class TinyType extends IntegerBaseType {
    public static final int TYPE_CODE = 1;
    public TinyType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
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
