package com.fang;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class IndexBuilder {

    private int EXP = 0; //0: never expire
    private MySqlAccess mysql;
    private MemcachedClient cache;


    IndexBuilder(String memcachedServer, int memcachedPortal, String mysqlHost, String mysqlDb, String mySqlUser, String mySqlPassWord) {
        try {
            mysql = new MySqlAccess(mysqlHost, mysqlDb, mySqlUser, mySqlPassWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String address = memcachedServer + ":" + memcachedPortal;
        try {
            cache = new MemcachedClient(new ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(), AddrUtil.getAddresses(address));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    void Close() {
        if (mysql != null) {
            try {
                mysql.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    Boolean buildInvertIndex(Ad ad) {
        String keyWords = Util.strJoin(ad.keyWords, ",");
        List<String> tokens = Util.cleanedTokenize(keyWords);
        for (String key : tokens) {
            if (cache.get(key) instanceof Set) {
                Set<Long> adIdList = (Set<Long>) cache.get(key);
                adIdList.add(ad.adId);
                cache.set(key, EXP, adIdList);
            } else {
                Set<Long> adIdList = new HashSet<>();
                adIdList.add(ad.adId);
                cache.set(key, EXP, adIdList);
            }
        }
        return true;
    }

    Boolean buildForwardIndex(Ad ad) {
        try {
            mysql.addAdData(ad);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    Boolean updateBudget(Campaign camp) {
        try {
            mysql.addCampaignData(camp);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
