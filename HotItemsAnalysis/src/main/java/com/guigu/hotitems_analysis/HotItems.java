package com.guigu.hotitems_analysis;

import com.guigu.hotitems_analysis.beans.ItemViewCount;
import com.guigu.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * 热门商品统计
 */

public class HotItems {
    public static void main(String[] args) throws Exception{
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.1 从文件中读取数据，创建DataStream
        //DataStreamSource<String> inputStream = env.readTextFile("C:\\GitHub\\2021\\Flink_UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //2.2 从kafka中读取数据
        //kafka配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.30.52:9092");
        properties.setProperty("group.id","consumer-item");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStream<String> inputStream
                = env.addSource(new FlinkKafkaConsumer<String>("hotItems",new SimpleStringSchema(),properties));


        //3 转换成POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0])
                    , new Long(fields[1])
                    , new Integer(fields[2])
                    , fields[3]
                    , new Long(fields[4]));
                })
                //顺序数据设置时间戳和workmark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp()*1000L;
                    }
        });

        //4 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                //过滤key值为pv的数据
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId")
                //开滑动窗口
                .timeWindow(Time.hours(1),Time.minutes(5))
                //计数窗口
                .aggregate(new ItemCountAgg(),new WindowItemCountResult())
                ;

        //5 收集同一窗口的所有商品count数据，排序输出top n
        windowAggStream
                //按照窗口结束时间分组
                .keyBy("windowEnd")
                //用自定义处理函数排序取前5
                .process(new TopNHotItems(5)).print();

        env.execute("hot items analysis");
    }

    //实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    //自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();

            collector.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String>{
        //定义topN的大小
        private int topSize;

        public TopNHotItems() {
        }

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        //定义列表状态，保存当前窗口内所有输出的ItemVidwCount
        private ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，当前已收集到的所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    if (o2.getCount()>o1.getCount()){
                        return 1 ;
                    }else if(o2.getCount()==o1.getCount()){
                        return 0 ;
                    }else{
                        return -1 ;
                    }
                }
            });

            //将排名信息格式化成String，方便打印输出
            StringBuffer sb = new StringBuffer();
            sb.append("====================================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n");
            //遍历列表，输出top n
            for (int i =0 ; i < Math.min(topSize,itemViewCounts.size());i++){
                ItemViewCount currentItenViewCount = itemViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" 商品id = ").append(currentItenViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItenViewCount.getCount())
                        .append("\n");
            }
            sb.append("====================================\n\n");

            //控制输出频率，每秒输入一次
            Thread.sleep(1000L);

            out.collect(sb.toString());


        }
    }
}
