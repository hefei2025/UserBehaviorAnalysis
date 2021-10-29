package com.guigu.hotitems_analysis;

import com.guigu.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsWithSQL {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2.1 从文件中读取数据，创建DataStream
        DataStreamSource<String> inputStream = env.readTextFile("C:\\GitHub\\2021\\Flink_UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //3 转换成POJO，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(
                    new Long(fields[0]),
                    new Long(fields[1]),
                    new Integer(fields[2]),
                    fields[3],
                    new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp()*1000L;
            }
        });

        //4 创建表执行环境,用blink版本
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        //5 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId,behavior,timestamp.rowtime as ts");

        //6 分组开窗
        //table api
        Table tableAgg = dataTable
                .filter("behavior='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 7 利用开窗函数，对count值进行排序并获取row number，得到top n
        //SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(tableAgg, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");
        Table resultTable = tableEnv.sqlQuery("select * from (select * , ROW_NUMBER() over (partition by windowEnd order by cnt desc ) as row_num from agg ) where row_num <=5");
        tableEnv.toRetractStream(resultTable,Row.class).print();

        env.execute("hot items anaylsis with SQL");


    }
}
