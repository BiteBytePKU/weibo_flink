package example;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;


public class SocialMediaAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Kafka参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.129.134:9092");
        properties.setProperty("group.id", "flink-group");
        String inputTopic = "weibo";
        String outputTopic = "analysis";

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
        DataStream<String> weiboStream = env.addSource(consumer);

        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
        */

        // 读取微博数据
//      DataStream<String> weiboStream = env.readTextFile("input/words.csv");

        // 读取敏感词库文件
        Set<String> sensitiveWords = readSensitiveWords("input/sensitive.csv");


        // 敏感词检测
        DataStream<Tuple3<String, String, String>> sensitiveWordsResult = weiboStream
                .flatMap(new SensitiveWordChecker(sensitiveWords));
        // 情感倾向分析
//        DataStream<Tuple3<String, String, String>> sentimentResult = weiboStream
//                .map(new SentimentAnalyzer());
//
//        sentimentResult.print();

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

    // 情感分析器
    public static class SentimentAnalyzer implements MapFunction<String, Tuple3<String, String, String>> {
        private StanfordCoreNLP pipeline;

        public SentimentAnalyzer() {
            // 创建 StanfordCoreNLP 对象
            Properties props = new Properties();
            props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
            pipeline = new StanfordCoreNLP(props);
        }

        @Override
        public Tuple3<String, String, String> map(String value) throws Exception {
            String[] columns = value.split(",");
            if (columns.length >= 3) {
                String id = columns[0];
                String bid = columns[1];
                String content = columns[2];

                // 分析情感
                String sentiment = analyzeSentiment(content);

                return new Tuple3<>(id, bid, sentiment);
            } else {
                return new Tuple3<>("", "", "无法分析");
            }
        }

        private String analyzeSentiment(String text) {
            int mainSentiment = 0;
            if (text != null && text.length() > 0) {
                int longest = 0;
                Annotation annotation = pipeline.process(text);
                for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                    int sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class));
                    int length = sentence.toString().length();
                    if (length > longest) {
                        mainSentiment = sentiment;
                        longest = length;
                    }
                }
            }
            return sentimentToString(mainSentiment);
        }

        private String sentimentToString(int sentiment) {
            switch (sentiment) {
                case 0: return "非常负面";
                case 1: return "负面";
                case 2: return "中性";
                case 3: return "正面";
                case 4: return "非常正面";
                default: return "未知";
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
