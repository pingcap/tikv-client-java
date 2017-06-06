package com.pingcap.tikv.type;


import com.pingcap.tikv.meta.TiColumnInfo;

public class MediumIntType extends IntegerBaseType {
    public static final int TYPE_CODE = 9;

    public MediumIntType(TiColumnInfo.InternalTypeHolder holder) {
        super(holder);
    }

    protected MediumIntType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    public int getTypeCode() {
        return TYPE_CODE;
    }

    public final static MediumIntType DEF_SIGNED_TYPE = new MediumIntType(false);
    public final static MediumIntType DEF_UNSIGNED_TYPE = new MediumIntType(true);
}
