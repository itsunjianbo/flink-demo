package com.roy.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态
 */
public class SumOperatorState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:8020/SumOperatorState"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        final DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);
        final SingleOutputStreamOperator<Integer> stream2 = stream.map(new MySumMapper("mysummapper"));
        stream2.print();
//        final DataStream<Integer> union = stream.union(stream2);
        env.execute("stream");
    }

    /**
     * CheckpointedFunction 算子操作接口
     */
    public static class MySumMapper implements MapFunction<Integer, Integer>, CheckpointedFunction {

        private int sum;
        private String stateKey;
        // 状态类型
        private ListState<Integer> checkpointedState;

        public MySumMapper(String stateKey) {
            this.stateKey = stateKey;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            return sum += value;
        }

        /**
         * 快照,有数据更新，清除，更新
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            checkpointedState.add(sum);
        }

        /**
         * 初始化状态
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>(
                    stateKey,
                    TypeInformation.of(new TypeHint<Integer>() {
                    }));
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {
                for (Integer subSum : checkpointedState.get()) {
                    sum += subSum;
                }
            }
        }
    }
}
