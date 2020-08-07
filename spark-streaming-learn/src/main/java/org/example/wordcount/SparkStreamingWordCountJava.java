package org.example.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by atyongsi@163.com on 2020/8/7
 */
public class SparkStreamingWordCountJava {
    public static void main(String[] args) throws InterruptedException {
        //创建运行环境SparkConf，任务的发起者SparkStreaming
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(3));

        //读取socke数据
        JavaReceiverInputDStream<String> datas = jsc.socketTextStream("localhost", 9999);

        //切分数据，统计数量
        JavaPairDStream<String, Integer> result = datas.flatMap( x-> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x1, x2) -> (x1 + x2));

        result.print();

        jsc.start();
        jsc.awaitTermination();

    }
}
