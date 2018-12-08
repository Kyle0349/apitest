package com.kyle.spark230.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {


    /**
     * 合并多个map
     * @param maps
     * @param <K>
     * @param <V>
     * @return
     * @throws Exception
     */
    public static <K, V> Map mergeMaps(Map<K, V>... maps) {
        Class clazz = maps[0].getClass(); // 获取传入map的类型
        Map<K, V> map = null;
        try {
            map = (Map) clazz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0, len = maps.length; i < len; i++) {
            map.putAll(maps[i]);
        }
        return map;
    }


    /**
     *
     * @param mapList
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map mergeMaps(List<Map<K, V>> mapList){
        Map<K, V> rtnMap = new HashMap<>();
        mapList.forEach(map -> rtnMap.putAll(map));
        return rtnMap;
    }
}
