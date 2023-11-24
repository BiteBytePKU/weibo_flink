package example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SocialMediaAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. 获取 有界流 的 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
        */

        // 2. 设置并行度
        env.setParallelism(1);

        // 3. 获取源数据（从远程机器上获取）
        DataStream<String> weiboStream = env.socketTextStream("192.168.10.111", 9999);
        // 读取微博数据
//      DataStream<String> weiboStream = env.readTextFile("input/words.csv");

        // 读取敏感词库文件
        Set<String> sensitiveWords = readSensitiveWords("input/sensitive.csv");


        // 敏感词检测
        DataStream<Tuple3<String, String, String>> sensitiveWordsResult = weiboStream
                .flatMap(new SensitiveWordChecker(sensitiveWords));

//        // 话题点赞数统计
//        DataStream<Tuple4<String, Long, Long, Long>> topicLikesResult = weiboStream
//                .flatMap(new TopicLikesExtractor())
//                .keyBy(0) // 根据话题进行分组
//                .timeWindow(Time.minutes(10)) // 10分钟的时间窗口
//                .aggregate(new LikesAggregate());

        sensitiveWordsResult.print();
//        topicLikesResult.print();

        env.execute("Weibo Analysis");
    }

    // 读取敏感词库
    private static Set<String> readSensitiveWords(String filePath) throws IOException {
        Set<String> sensitiveWords = new HashSet<>();
        // 读取文件的单行内容
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(line -> sensitiveWords.addAll(Arrays.asList(line.split(","))));
        }
        // 使用逗号分割字符串，然后添加到集合中
//        sensitiveWords.addAll(Arrays.stream(line.split(",")).collect(Collectors.toSet()));
        return sensitiveWords;
    }

    // 敏感词检测器
    public static class SensitiveWordChecker implements FlatMapFunction<String, Tuple3<String, String, String>> {
        private Set<String> sensitiveWords;

        public SensitiveWordChecker(Set<String> sensitiveWords) {
            this.sensitiveWords = sensitiveWords;
        }

        @Override
        public void flatMap(String value, Collector<Tuple3<String, String, String>> out) {
            try {
                String[] columns = value.split(",");
                System.out.println("__"+columns.length+"__"+value);
                if (columns.length >= 3) {
                    String id = columns[0];
                    String bid = columns[1];
                    String content = columns[2];

                    for (String word : sensitiveWords) {
                        if (content.contains(word)) {
                            out.collect(new Tuple3<>(id, bid, word));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing: " + value);
                e.printStackTrace();
            }
        }
    }

    // 话题和点赞数提取器
    public static class TopicLikesExtractor implements FlatMapFunction<String, Tuple3<String, Long, Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<String, Long, Long>> out) {
            String[] columns = value.split(",");
            String topic = columns[3]; // 假设话题在第4列
            long likes = Long.parseLong(columns[4]); // 假设点赞数在第5列
            out.collect(new Tuple3<>(topic, likes, 1L));
        }
    }

    // 对点赞数进行聚合的函数
    public static class LikesAggregate implements AggregateFunction<Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Long>, Tuple4<String, Long, Long, Long>> {
        @Override
        public Tuple4<String, Long, Long, Long> createAccumulator() {
            return new Tuple4<>("", 0L, 0L, 0L); // 话题名称，总点赞数，最高点赞数，计数
        }

        @Override
        public Tuple4<String, Long, Long, Long> add(Tuple3<String, Long, Long> value, Tuple4<String, Long, Long, Long> accumulator) {
            return new Tuple4<>(value.f0, accumulator.f1 + value.f1, Math.max(accumulator.f2, value.f1), accumulator.f3 + 1);
        }

        @Override
        public Tuple4<String, Long, Long, Long> getResult(Tuple4<String, Long, Long, Long> accumulator) {
            long avgLikes = accumulator.f1 / accumulator.f3; // 计算平均点赞数
            return new Tuple4<>(accumulator.f0, avgLikes, accumulator.f2, accumulator.f3);
        }

        @Override
        public Tuple4<String, Long, Long, Long> merge(Tuple4<String, Long, Long, Long> a, Tuple4<String, Long, Long, Long> b) {
            return new Tuple4<>(a.f0, a.f1 + b.f1, Math.max(a.f2, b.f2), a.f3 + b.f3);
        }
    }

}
