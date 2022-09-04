package com.roy.flink.project.userlogin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * CEP 案例
 * @author roy
 * @date 2021/9/17
 * @desc 十秒钟内连续登陆失败的用户分析。使用Flink CEP进行快速模式匹配
 */
public class UserLoginAna {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L); //BoundedOutOfOrdernessWatermarks定时提交Watermark的间隔

        //从kafka读取数据 转换成GiftRecord对象
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
//        properties.setProperty("group.id", "gift");
//        final FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties);
//        kafkaConsumer.setStartFromLatest();
//        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        //使用Socket测试。
        env.setParallelism(1);
        final DataStreamSource<String> dataStream = env.socketTextStream("worker1", 7777);
        final SingleOutputStreamOperator<UserLoginRecord> userLoginRecordStream = dataStream.map(new MapFunction<String, UserLoginRecord>() {
            @Override
            public UserLoginRecord map(String value) throws Exception {
                final String[] splitVal = value.split(",");
                return new UserLoginRecord(splitVal[0], Integer.parseInt(splitVal[1]), Long.parseLong(splitVal[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserLoginRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserLoginRecord>) (element, recordTimestamp) -> element.getLoginTime())
        );

        // 第一种实现
        //Flink CEP定义消息匹配器。 10秒钟内出现三次登陆失败的记录(不一定连续)
        final Pattern<UserLoginRecord, UserLoginRecord> pattern = Pattern.<UserLoginRecord>begin("start").where(new SimpleCondition<UserLoginRecord>() {
            @Override
            public boolean filter(UserLoginRecord value) throws Exception {
                return 1 == value.getLoginRes();
            }
        }).times(3).within(Time.seconds(10)); // 10s 内连续3次登陆失败
        // times()指定模式期望匹配到的事件出现次数在#fromTimes和#toTimes之间.within 定义匹配模式的事件序列出现的最大时间间隔。如果未完成的事件序列超过了这个事件， 就会被丢弃

        // 第二种实现
        //连续三次登陆失败。next表示连续匹配。 不连续匹配使用followBy
//        final Pattern<UserLoginRecord, UserLoginRecord> pattern = Pattern.<UserLoginRecord>begin("one").where(new SimpleCondition<UserLoginRecord>() {
//            @Override
//            public boolean filter(UserLoginRecord value) throws Exception {
//                return 1 == value.getLoginRes();
//            }
//        }).next("two").where(new SimpleCondition<UserLoginRecord>() {
//            @Override
//            public boolean filter(UserLoginRecord value) throws Exception {
//                return 1 == value.getLoginRes();
//            }
//        }).next("three").where(new SimpleCondition<UserLoginRecord>() {
//            @Override
//            public boolean filter(UserLoginRecord value) throws Exception {
//                return 1 == value.getLoginRes();
//            }
//        }).within(Time.seconds(10));

        // PatternStream
        final PatternStream<UserLoginRecord> badUser = CEP.pattern(userLoginRecordStream, pattern);

        final MyProcessFunction myProcessFunction = new MyProcessFunction();
        final SingleOutputStreamOperator<UserLoginRecord> badUserStream = badUser.process(myProcessFunction);

        badUserStream.print("badUser");
        env.execute("UserLoginAna");
    }

    //    public static class MyProcessFunction extends PatternProcessFunction<UserLoginRecord,UserLoginRecord> implements TimedOutPartialMatchHandler {
    public static class MyProcessFunction extends PatternProcessFunction<UserLoginRecord, UserLoginRecord> {

        @Override
        public void processMatch(Map<String, List<UserLoginRecord>> match, Context ctx, Collector<UserLoginRecord> out) throws Exception {
            final List<UserLoginRecord> records = match.get("start");
            for (UserLoginRecord record : records) {
                out.collect(record);
            }
        }
        //TimedOutPartialMatchHandler将超时的匹配结果输出到侧输出流。
//        @Override
//        public void processTimedOutMatch(Map match, Context ctx) throws Exception {
//            ctx.output();
//        }
    }

    // TODO 基础继承实现
//    public static class MyProcessFunction1 extends PatternProcessFunction<UserLoginRecord, UserLoginRecord> {
//
//        @Override
//        public void processMatch(Map<String, List<UserLoginRecord>> map, Context context, Collector<UserLoginRecord> collector) throws Exception {
//
//        }
//    }

}
