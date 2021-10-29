package networkflow_analysis;

import networkflow_analysis.beans.ApacheLogEvent;
import networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception{
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 设置时间语义-事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //3 读取文件，并转换成POJO 分配时间戳和watermark
        URL resource = HotPages.class.getResource("/apache.log");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestmp = simpleDateFormat.parse(fields[3]).getTime();

            return new ApacheLogEvent(fields[0], fields[1], timestmp, fields[5], fields[6]);
        })
         //乱序数据设置时间戳和watermark,并设置延时时间为1分钟
         .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });

        //4 分组开窗聚合
        //定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};
        SingleOutputStreamOperator<PageViewCount> windowAggStrean = dataStream
                //过滤请求方法为get的url
                .filter(data -> "get".equalsIgnoreCase(data.getMethod()))
                //过滤掉资源文件的url
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|jpg|png|ico)$).)*$";
                    return Pattern.matches(regex,data.getUrl());
                })
                //通过url做分组
                .keyBy(ApacheLogEvent::getUrl)
                //开滑动窗口
                .timeWindow(Time.minutes(10), Time.seconds(5))
                //允许迟到时间
                .allowedLateness(Time.minutes(1))
                //将超时数据放入侧输出流中
                .sideOutputLateData(lateTag)
                //聚合
                .aggregate(new PageCountAgg(), new WindowPageCountResult());

        //5 收集同一窗口count数据，排序输出
        windowAggStrean
                //按照窗口结束时间分组
                .keyBy(PageViewCount::getWindowEnd)
                //用自定义处理函数排序取top N
                .process(new TopNHotPages(5)).print();

        windowAggStrean.getSideOutput(lateTag).print("late data");

        //6 执行
        env.execute("hot pages analysis");

    }

    //实现自定义增量聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    //自定义全窗口函数
    public static class WindowPageCountResult implements WindowFunction<Long,PageViewCount,String, TimeWindow>{

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            String url = s ;
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();

            collector.collect(new PageViewCount(url,windowEnd,count));
        }
    }

    public static class TopNHotPages extends KeyedProcessFunction<Long,PageViewCount,String>{
        //定义top n的大小
        private int topSize;
        public TopNHotPages(int topSize){
            this.topSize = topSize;
        }

        //定义列表状态，保存当前窗口内所有输出的PageViewCount
        //private ListState<PageViewCount> pageViewCountListState;
        //优化程序，将状态存放在map中
        private MapState<String , Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
           /* pageViewCountListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<PageViewCount>("page-view-count-list",PageViewCount.class));*/
           pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-view-count-map",String.class,Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            //每来一条数据，存入List中，并注册定时器
            //pageViewCountListState.add(pageViewCount);
            pageViewCountMapState.put(pageViewCount.getUrl(),pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);

            //注册一个一分钟后的定时器，用来清空状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+ 60*1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，当前已收集到的所有数据，排序输出
            //ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
           /* pageViewCounts.sort(new Comparator<PageViewCount>() {
                @Override
                public int compare(PageViewCount o1, PageViewCount o2) {
                    if (o2.getCount()>o1.getCount()){
                        return 1 ;
                    }else if(o2.getCount()==o1.getCount()){
                        return 0 ;
                    }else{
                        return -1 ;
                    }
                }
            });*/

           //先判断是否到了窗口关闭清理的时间，如果是，直接清空状态返回
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L){
                pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String,Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if (o2.getValue()>o1.getValue()){
                        return 1 ;
                    }else if(o2.getValue()==o1.getValue()){
                        return 0 ;
                    }else{
                        return -1 ;
                    }
                }
            });


            //将排名信息格式化成String ,方便打印输出
            StringBuffer sb = new StringBuffer();
            sb.append("====================================\n");
            sb.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n");
            //遍历列表，输出top n
            for (int i =0 ; i < Math.min(topSize,pageViewCounts.size());i++){
                /*PageViewCount currentItenViewCount = pageViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" 热门度 = ").append(currentItenViewCount.getCount())
                        .append(" 被访问url = ").append(currentItenViewCount.getUrl())
                        .append("\n");*/

                Map.Entry<String, Long> currentItenViewCount = pageViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" 热门度 = ").append(currentItenViewCount.getValue())
                        .append(" 被访问url = ").append(currentItenViewCount.getKey())
                        .append("\n");
            }
            sb.append("====================================\n");

            //控制输出频率，每秒输入一次
            Thread.sleep(1000L);

            out.collect(sb.toString());
        }
    }
}
