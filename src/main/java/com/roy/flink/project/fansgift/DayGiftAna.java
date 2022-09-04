package com.roy.flink.project.fansgift;

import com.roy.flink.project.fansgift.FansGiftResult;
import com.roy.flink.project.fansgift.GiftRecord;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 案例 ：贡献日榜计算程序
 *
 * @author roy
 * @date 2021/9/15
 */
public class DayGiftAna {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // BoundedOutOfOrdernessWatermarks定时提交Watermark的间隔
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 状态后端，给所有任务进行同步
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:8020/dayGiftAna"));

        // 从kafka读取数据 转换成GiftRecord对象
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
//        properties.setProperty("group.id", "gift");
//        final FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties);
//        kafkaConsumer.setStartFromLatest();
//        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        //使用Socket测试。
        env.setParallelism(1);
        final DataStreamSource<String> dataStream = env.socketTextStream("hadoop01", 7777);

        final SingleOutputStreamOperator<FansGiftResult> fansGiftResult = dataStream.map((MapFunction<String, GiftRecord>) value -> {
                    final String[] valueSplit = value.split(",");
                    //SimpleDateFormat 多线程不安全。
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    final long giftTime = sdf.parse(valueSplit[3]).getTime();
                    return new GiftRecord(valueSplit[0], valueSplit[1], Integer.parseInt(valueSplit[2]), giftTime);
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        // 设置水位线，2分钟延迟
                        .<GiftRecord>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                        // 指定某个字段为事件时间
                        .withTimestampAssigner((SerializableTimestampAssigner<GiftRecord>) (element, recordTimestamp) -> element.getGiftTime()))
                // 根据主播id和粉丝id进行分组
                .keyBy((KeySelector<GiftRecord, String>) value -> value.getHostId() + "_" + value.getFansId()) //按照HostId分组
                // 10分钟开一个窗口
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                // 延迟等待时间 2秒
                .allowedLateness(Time.seconds(2))
                // 进行聚合  WinodwGiftRecordAgg对并行任务数据进行合并  AllWindowGiftRecordAgg 对开窗后的并行任务进行合并
                .aggregate(new WinodwGiftRecordAgg(), new AllWindowGiftRecordAgg());
        //打印结果测试
        fansGiftResult.print("fansGiftResult");


        //将结果存入到ES
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop02", 9200));
        final ElasticsearchSink.Builder<FansGiftResult> esBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<FansGiftResult>() {
                    @Override
                    public void process(FansGiftResult fansGiftResult, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("hostId", fansGiftResult.getHostId());
                        data.put("fansId", fansGiftResult.getFansId());
                        data.put("giftCount", fansGiftResult.getGiftCount());
                        data.put("windowEnd", fansGiftResult.getWindowEnd());
                        
                        final IndexRequest esRequest = Requests.indexRequest()
                                .index("daygiftanalyze")
                                .id("" + System.currentTimeMillis())
                                .source(data);
                        requestIndexer.add(esRequest);
                    }
                });
        esBuilder.setBulkFlushMaxActions(1); //每一条记录直接写入到ES，不做本地缓存。
        esBuilder.setBulkFlushBackoffRetries(3); //写入失败后的重试次数。
//        esBuilder.setFailureHandler(new ActionRequestFailureHandler() { //失败处理策略
//            @Override
//            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
////                requestIndexer.add();  //重新加入请求。
//            }
//        });
        final ElasticsearchSink<FansGiftResult> esSink = esBuilder.build();
        fansGiftResult.addSink(esSink);

        env.execute("DayGiftAna");
    }


    //在每个子任务中将窗口期内的礼物进行累计合并
    //增加状态后端。
    /**
     * IN : GiftRecord 单挑数据
     * ACC : Long 第一个 累计的数量
     * OUT : Long 第二个 输出的数量
     */
    private static class WinodwGiftRecordAgg implements AggregateFunction<GiftRecord, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 每进入1条数据，进行累加
        @Override
        public Long add(GiftRecord value, Long accumulator) {
            Long res = accumulator + value.getGiftCount();
            return res;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        // 聚合的，照样写就行了
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //对窗口期内的所有子任务进行窗口聚合操作。

    /**
     * IN  : Long 传入的类型，就是前面的方法计算的数据
     * OUT : FansGiftResult 输出的类型
     * KEY : 就是分组的key
     * W : 窗口类型 ，TimeWindow 是时间窗口
     */
    private static class AllWindowGiftRecordAgg extends RichWindowFunction<Long, FansGiftResult, String, TimeWindow> {

        ValueState<FansGiftResult> state;

        // 计算
        //  void apply(KEY var1, W var2, Iterable<IN> var3, Collector<OUT> var4) throws Exception;
        @Override
        public void apply(String s, TimeWindow window, java.lang.Iterable<Long> input, Collector<FansGiftResult> out) throws Exception {
            final String[] splitKey = s.split("_");
            String hostId = splitKey[0];
            String fansId = splitKey[1];

            // 获取礼物数
            final Long giftCount = input.iterator().next();
            // 获取窗口结束时间
            final long windowEnd = window.getEnd();
            // 初始化返回对象
            final FansGiftResult fansGiftResult = new FansGiftResult(hostId, fansId, giftCount, windowEnd);
            // 输出结果
            out.collect(fansGiftResult);
            // 把结果存到状态里面
            state.update(fansGiftResult);
        }

        // 计算的时候，从运行环境中进行加载
        @Override
        public void open(Configuration parameters) throws Exception {
            final ValueStateDescriptor<FansGiftResult> stateDescriptor = new ValueStateDescriptor<>("WinodwGiftRecordAgg", TypeInformation.of(new TypeHint<FansGiftResult>() {
            }));
            state = this.getRuntimeContext().getState(stateDescriptor);
        }
    }
}
