package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkBatch {
    public static void main(String[] args) throws Exception {
        // 1. 通过 单例模式 获取批处理的 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
        */

        // 2. 获取源数据 (从文件一行一行读取)
        DataSource<String> dataSource = env.readTextFile("input/words.csv");

        // 3. 数据处理
        dataSource
                // (1) 一对多：将一行数据映射为多个单词
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        //  1) 对该行数据切割为一个一个的单词
                        String[] words = line.split(",");
                        //  2) 将单词加入到收集器中
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                // (2) 一对一：将单词 word 映射为 (word, 1) 形式。
                //     这一个算子其实可以省略，在flatMap中返回out.collect(Tuple2.of(word, lL));即可
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                })
                // (3) 分组统计
                .groupBy(0)  // DataSet API 的算子
                .sum(1)
                // (4) 打印统计结果
                .print();
    }
}
