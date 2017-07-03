package com.pingcap.tikv;


import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.util.Pair;

import java.util.HashMap;

/**
 * Created by zhaoziming on 2017/6/22.
 */
public class TikvClient {

    RegionManager regionManager;
    TiSession session;
    HashMap<String, RegionStoreClient> clients =  new HashMap<String, RegionStoreClient>();

    public TikvClient(String host) {
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of(host));
        session = TiSession.create(conf);
        PDClient client = PDClient.createRaw(session);
        regionManager = new RegionManager(client);
    }

    public void set(String key, byte[] value) {
        Pair<RegionStoreClient, Kvrpcpb.Context> pair = checkAndGetClient(key);
        long start = System.nanoTime();
        pair.first.rawPut(ByteString.copyFrom(key.getBytes()), ByteString.copyFrom(value),pair.second);
        long end = System.nanoTime();
        System.out.println("rawput:" + (end - start) / 1000000.0 + "ms");
    }

    public byte[] get(String key) {
        Pair<RegionStoreClient, Kvrpcpb.Context> pair = checkAndGetClient(key);
        ByteString rawgetresp = pair.first.rawGet(ByteString.copyFrom(key.getBytes()), pair.second);
        return rawgetresp.toByteArray();
    }

    public void del(String key) {
        Pair<RegionStoreClient, Kvrpcpb.Context> pair = checkAndGetClient(key);
        pair.first.rawDelete(ByteString.copyFrom(key.getBytes()),pair.second);
    }


    Pair<RegionStoreClient, Kvrpcpb.Context> checkAndGetClient(String key) {
        long start = System.nanoTime();

        Pair<Metapb.Region, Metapb.Store> pair = regionManager.getRegionStorePairByKey(ByteString.copyFrom(key.getBytes()));

        //System.out.println("create client");
        RegionStoreClient storeClient = RegionStoreClient.create(pair.first, pair.second, session, regionManager);

        Metapb.Region region = pair.first;
        Kvrpcpb.Context context = Kvrpcpb.Context.newBuilder()
                .setRegionId(region.getId())
                .setRegionEpoch(region.getRegionEpoch())
                .setPeer(region.getPeers(0))
                .build();
        long end = System.nanoTime();
        //System.out.println("get client:"+(end -start) / 1000000.0 + "ms");

        return Pair.create(storeClient, context);
    }

}
