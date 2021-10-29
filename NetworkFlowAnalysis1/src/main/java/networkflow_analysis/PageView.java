package networkflow_analysis;

import com.guigu.common.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

public class PageView {
    public static void main(String[] args) throws Exception{

        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3 设置并发度
        env.setParallelism(1);
        //4 从文件中读取数据,
        URL resource = PageView.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        //5 转换成POJO,分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0])
                    , new Long(fields[1])
                    , new Integer(fields[2])
                    , fields[3]
                    , new Long(fields[4]));

        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        //6 开窗聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = dataStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<String, Long>("pv", 1L);
            }
        }).keyBy(0).timeWindow(Time.hours(1)).sum(1);

        //7 打印输出
        resultStream.print();

        //8 执行
        env.execute();


    }
}
