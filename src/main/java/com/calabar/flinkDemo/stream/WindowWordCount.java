package com.calabar.flinkDemo.stream;

/**
 * <p/>
 * <li>@author: jyj019> </li>
 * <li>Date: 2018/9/3 11:37</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlatMapFunction<String, Tuple2<String, Integer>> spliter = (String sentence, Collector<Tuple2<String, Integer>> out) -> {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        };
        FilterFunction<Tuple2<String, Integer>> filterFunction = (Tuple2<String, Integer> out) -> {
            if (out.f0.contains("joe")) {
                return true;
            }
            return false;
        };
        DataStream<Tuple2<String, Integer>> dataStream = env
                //.addSource(new RandomEventSource(5).closeDelay(1500));
                .socketTextStream("localhost", 8089)
//                .flatMap(new Splitter())
                .flatMap(spliter).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .filter(filterFunction)
                .keyBy(0)
                .sum(1);
//                .timeWindow(Time.seconds(5))
//        dataStream.addSink();
        dataStream.print();
        env.execute();
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
