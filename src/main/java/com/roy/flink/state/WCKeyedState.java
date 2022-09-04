package com.roy.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author roy
 * @date 2021/9/6
 */
public class WCKeyedState {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Tuple2<String, Integer>> stream = environment.fromCollection(Arrays.asList(Tuple2.of("a", 1), Tuple2.of("a", 5), Tuple2.of("a", 7), Tuple2.of("a", 2)
                , Tuple2.of("b", 2), Tuple2.of("b", 6), Tuple2.of("b", 3), Tuple2.of("b", 8)
                , Tuple2.of("c", 4), Tuple2.of("c", 8), Tuple2.of("c", 4), Tuple2.of("c", 6)));
        //按照字符分组
        final KeyedStream<Tuple2<String, Integer>, String> keyedStream = stream.keyBy((key) -> key.f0);
        keyedStream.flatMap(new WCFlatMapFunction("WCKeyedState")).print();

        environment.execute("WCKeyedState");
    }

    public static class WCFlatMapFunction extends RichFlatMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>{

        private String stateDesc;
        ValueState<Tuple2<String, Integer>> valueState;

        public WCFlatMapFunction(String stateDesc) {
            this.stateDesc = stateDesc;
        }

        @Override
        public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> wordCountList = valueState.value();
            if(null == wordCountList){
                wordCountList = input;
            }else{
                wordCountList.f1+= input.f1;
            }
            valueState.update(wordCountList);
            out.collect(wordCountList);
//            valueState.clear();
        }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Tuple2<String,Integer>> descriptor =
                new ValueStateDescriptor<>(stateDesc,
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
            //设置状态的存活时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            descriptor.enableTimeToLive(ttlConfig);
            valueState = this.getRuntimeContext().getState(descriptor);
            //另外几种state类型
//            this.getRuntimeContext().getMapState();
//            this.getRuntimeContext().getListState();
//            this.getRuntimeContext().getAggregatingState();
//            this.getRuntimeContext().getReducingState();
        }

    }
}
