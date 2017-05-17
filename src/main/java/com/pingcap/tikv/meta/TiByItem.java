package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.ByItem;




public class TiByItem {

    public ByItem toProto() {
        ByItem.Builder builder = ByItem.newBuilder();
        return builder.build();
    }
}
