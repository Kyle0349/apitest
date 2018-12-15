package com.kyle.spark230.utils;

import com.esotericsoftware.kryo.Kryo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;


/**
 * 解决kafka direct dstream 序列化问题
 */
public class MyRegistrator implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ConsumerRecord.class);
    }
}
