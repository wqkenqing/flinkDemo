package com.calabar.flinkDemo.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/7/31
 * @desc flinkSource
 */
public class FlinkSourceDemo {
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void fromCollection() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(i + "");
        }
        DataStream<String> dstrem = env.fromCollection(list);
        dstrem.map(s -> new Tuple2(s, 1)).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .print();
    }

    public static void fromGenerateSequence() {
        DataStreamSource<Long> interStream = env.generateSequence(10, 1000);
        interStream.print();
    }

    public static void fromTextFile() {
        DataStream<String> text = env.readTextFile("/Users/wqkenqing/Desktop/out/keyCount.txt");
        FlatMapFunction<String, Tuple2<String, String>> spliter = (String sentence, Collector<Tuple2<String, String>> out) -> {
            String ss[] = sentence.split("\\s+");
            out.collect(new Tuple2<String, String>(ss[0], ss[1]));
        };
        FilterFunction<Tuple2<String, String>> filter = (Tuple2<String, String> message) -> {
            if (message.f1.contains("m")) {
                return true;
            }
            return false;
        };
        ReduceFunction<String> reduceFunction = (m1, m2) -> {
            return m1 + m2;
        };
        text.flatMap(spliter).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class))
                .keyBy(0)
                .reduce(reduceFunction)
                .print();
    }

    public static void kafkaSourceConnect() {

    }


    public static  void fromHdfsFile() {

        Configuration config = new Configuration();
//
//        OrcTableSource orcTableSource = OrcTableSource.builder()
//                // path to ORC file(s)
//                .path(fileName)
//                // schema of ORC files
//                .forOrcSchema(Schema.schema)
//                // Hadoop configuration
//                .withConfiguration(config)
//                // build OrcTableSource
//                .build();
        DataStream<String> text = env.readTextFile("hdfs://ucloud1:9000/files/zookeeper.out");
        text.print();
    }
    public static void main(String[] args) throws Exception {
        fromTextFile();
        env.execute();
    }
}
