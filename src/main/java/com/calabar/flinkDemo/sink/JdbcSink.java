package com.calabar.flinkDemo.sink;

import com.calabar.flinkDemo.model.Toll;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.mortbay.util.ajax.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/8/3
 * @desc
 */
public class JdbcSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map properties = new HashMap();
        properties.put("bootstrap.servers", "192.168.10.214:9092");
        properties.put("group.id", "demo1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("topic", "jzw_toll_island_info");
        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        FlinkKafkaConsumer010 consumer010 = new FlinkKafkaConsumer010(
                parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());
        DataStream<String> messageStream = env.addSource(consumer010);
        DataStream<Toll> tstream = messageStream.map(s -> {
            Toll toll = com.alibaba.fastjson.JSON.parseObject(s, Toll.class);
            return toll;
        });
        tstream.rebalance().addSink(new MysqlJdbcSink());
        env.execute();
    }
}
