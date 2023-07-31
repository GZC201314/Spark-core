package org.gzc.bigdata.spark.javacore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggTest {
    public static void main(String[] args) {
        // 创建Spark配置对象
        SparkConf conf = new SparkConf()
                .setAppName("SparkAggregateDemo")
                .setMaster("local[*]");

        // 创建JavaSparkContext对象
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // 创建一个RDD
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        // 使用aggregate函数计算RDD元素的和\
        List<Integer> integers = new ArrayList<>();
        List<Integer> aggregate = rdd.aggregate(integers, new Function2<List<Integer>, Integer, List<Integer>>() {
                    @Override
                    public List<Integer> call(List<Integer> integers, Integer integer) throws Exception {
                        System.out.println(integers + "    " + Thread.currentThread().getName() + "   " + integer);
                        integers.add(integer);

                        return integers;
                    }
                }, new Function2<List<Integer>, List<Integer>, List<Integer>>() {
                    @Override
                    public List<Integer> call(List<Integer> integers, List<Integer> integers2) throws Exception {
                        System.out.println(integers + "    "+Thread.currentThread().getName() + "     "+ integers2);
                        integers.addAll(integers2);
                        return integers;
                    }
                }

        );

        // 输出结果
        System.out.println("Sum: " + aggregate);
        sparkContext.close();
    }
}
