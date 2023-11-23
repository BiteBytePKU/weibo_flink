package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class SocialMediaAnalysis {
    public static void main(String[] args) throws Exception {
        // 1. 获取 有界流 的 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
        */

        // 2. 设置并行度
        env.setParallelism(1);

        // 3. 获取源数据（从远程机器上获取）
        DataStream<String> weiboStream = env.socketTextStream("192.168.10.111", 9999);


        // 4. 数据处理
        DataStream<SocialMediaAnalysis.PostStats> postStats = weiboStream.map(new MapFunction<String, SocialMediaAnalysis.PostStats>() {
            @Override
            public PostStats map(String value) throws Exception {
                String[] fields = value.split(",");
                long likes = Long.parseLong(fields[9]);
                long comments = Long.parseLong(fields[10]);
                long shares = Long.parseLong(fields[11]);
                return new PostStats(likes, comments, shares);
            }
        });

        // 5. 执行(流数据处理必须有这一步)
        env.execute("WeiboStream"); // 传入的字符串是设置该job的名字，在WebUI界面会显示
    }
//    读取敏感词库
    private static Set<String> readSensitiveWords(String filePath) throws IOException, IOException {
        Set<String> sensitiveWords = new HashSet<>();
        // 读取文件的单行内容
        String line = Files.lines(new File(filePath).toPath()).findFirst().orElse("");

        // 使用逗号分割字符串，然后添加到集合中
        sensitiveWords.addAll(Arrays.stream(line.split(",")).collect(Collectors.toSet()));

        return sensitiveWords;
    }
    public static class PostStats {
        public long likes;
        public long comments;
        public long shares;

        public PostStats(long likes, long comments, long shares) {
            this.likes = likes;
            this.comments = comments;
            this.shares = shares;
        }
    }
    public static class SensitiveWordChecker implements FlatMapFunction<String, Tuple3<String, String, String>> {
        private Set<String> sensitiveWords;

        public SensitiveWordChecker(Set<String> sensitiveWords) {
            this.sensitiveWords = sensitiveWords;
        }

        @Override
        public void flatMap(String value, Collector<Tuple3<String, String, String>> out) {
            String[] columns = value.split(",");
            String id = columns[0];
            String bid = columns[1];
            String content = columns[2];

            for (String word : sensitiveWords) {
                if (content.contains(word)) {
                    out.collect(new Tuple3<>(id, bid, word));
                }
            }
        }
    }
}


class SocialMediaAnalysis2 {

    public static void main(String[] args) throws Exception {
        // 初始化环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取 CSV 文件
        DataStream<String> csvDataStream = env.readTextFile("path/to/your/words.csv");

        // 解析 CSV 数据
        DataStream<PostStats> postStats = csvDataStream.map(new MapFunction<String, PostStats>() {
            @Override
            public PostStats map(String value) throws Exception {
                String[] fields = value.split(",");
                long likes = Long.parseLong(fields[9]);
                long comments = Long.parseLong(fields[10]);
                long shares = Long.parseLong(fields[11]);
                return new PostStats(likes, comments, shares);
            }
        });

        // 分析数据

        env.execute("Social Media Analysis");
    }

    // 定义映射 CSV 行的类
    public static class PostStats {
        public long likes;
        public long comments;
        public long shares;

        public PostStats(long likes, long comments, long shares) {
            this.likes = likes;
            this.comments = comments;
            this.shares = shares;
        }
    }
    //    public static void main(String[] args) throws Exception {
//        // 1. 获取 有界流 的 执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 10000);
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
//        */
//
//        // 2. 设置并行度
//        env.setParallelism(1);
//
//        // 3. 获取源数据（从远程机器上获取）
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("192.168.10.111", 9999);
//
//        // 4. 数据处理
//        dataStreamSource
//                // (1) 一对多：将一行数据映射为多个单词，同时做一对一映射转换：将单词 word 映射为 (word, 1) 形式。
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//                    @Override
//                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
//                        //  1) 对该行数据切割为一个一个的单词
//                        String[] words = line.split(",");
//                        //  2) 将单词以元组(word, 1)的形式加入到收集器中
//                        for (String word : words) {
//                            out.collect(Tuple2.of(word, 1L));
//                        }
//                    }
//                })
//                // (3) 分组统计
//                // 后面的String是打上的标签类型，后面会根据标签分组。
//                // 因为这里是根据tuple2.f0字段分组，所以标签类型和tuple2.f0类型相同
//                .keyBy(new KeySelector<Tuple2<String, Long>, String>() { // DataStream API 的算子
//                    @Override
//                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
//                        return tuple2.f0;  // f0是指元组的0下标字段
//                    }
//                })
//                .sum(1)
//                .print();
//
//        // 5. 执行(流数据处理必须有这一步)
//        env.execute("UnboundStream"); // 传入的字符串是设置该job的名字，在WebUI界面会显示
//    }
}
