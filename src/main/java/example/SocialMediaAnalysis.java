package example;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), properties);
        DataStream<String> weiboStream = env.addSource(consumer);

        /* 如果需要设置固定的WebUI端口，则在获取执行环境时需要传入参数
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment(conf);
        */

        // 读取关键词库文件
        InputStream inputStream = SocialMediaAnalysis.class.getClassLoader().getResourceAsStream("sensitive.csv");
        Set<String> sensitiveWords = readSensitiveWords(inputStream);


        // 关键词检测
        DataStream<Tuple3<String, String, String>> sensitiveWordsResult = weiboStream
                .flatMap(new SensitiveWordChecker(sensitiveWords));

        // 情感倾向分析
        DataStream<Tuple3<String, String, String>> sentimentResult = weiboStream
                .map(new SentimentAnalyzer());


       // 微博点赞评论转发热度分析
        DataStream<Tuple4<String, String,String,String>> topicLikesResult = weiboStream
                .flatMap(new TopicLikesExtractor());

        String outputTopic = "flinkresult";
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "192.168.129.134:9091");

        FlinkKafkaProducer<Tuple3<String, String, String>> myProducer1 = new FlinkKafkaProducer<>(
                outputTopic,                  // 目标 Kafka 主题
                new KafkaTuple3Schema(),        // 自定义的 Tuple3 到 String 的序列化 schema
                properties                      // Kafka 配置属性
        );
        FlinkKafkaProducer<Tuple3<String, String, String>> myProducer2 = new FlinkKafkaProducer<>(
                outputTopic,                  // 目标 Kafka 主题
                new KafkaTuple3Schema(),        // 自定义的 Tuple3 到 String 的序列化 schema
                properties                      // Kafka 配置属性
        );
        FlinkKafkaProducer<Tuple4<String, String,String, String>> myProducer3 = new FlinkKafkaProducer<>(
                outputTopic,                  // 目标 Kafka 主题
                new KafkaTuple4Schema(),        // 自定义的 Tuple4 到 String 的序列化 schema
                properties                      // Kafka 配置属性
        );

        sensitiveWordsResult.writeAsCsv("key_result.csv");
        sentimentResult.writeAsCsv("sentiment_result.csv");
        topicLikesResult.writeAsCsv("tp_result.csv");
        // 将结果发送到 Kafka
        sensitiveWordsResult.addSink(myProducer1).name("Kafka Sink - FlinkResult");
        sentimentResult.addSink(myProducer2).name("Kafka Sink - FlinkResult");
        topicLikesResult.addSink(myProducer3).name("Kafka Sink - FlinkResult");
        env.execute("Weibo Analysis");

    }


    public static class KafkaTuple3Schema implements SerializationSchema<Tuple3<String, String, String>> {
        @Override
        public byte[] serialize(Tuple3<String, String, String> element) {
            // 自定义如何将 Tuple3 转换为字节数组
            String result = element.f0 + "," + element.f1 + "," + element.f2;
            return result.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class KafkaTuple4Schema implements SerializationSchema<Tuple4<String, String,String, String>> {
        @Override
        public byte[] serialize(Tuple4<String, String, String,String> element) {
            // 自定义如何将 Tuple3 转换为字节数组
            String result = element.f0 + "," + element.f1 + "," + element.f2+ "," + element.f3;
            return result.getBytes(StandardCharsets.UTF_8);
        }
    }

    private static Set<String> readSensitiveWords(InputStream inputStream) throws IOException {
        Set<String> sensitiveWords = new HashSet<>();

        // 使用 BufferedReader 从 InputStream 读取数据
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 使用逗号分割字符串，然后添加到集合中
                sensitiveWords.addAll(Arrays.asList(line.split(",")));
            }
        }
        return sensitiveWords;
    }


    // 关键词检测器
    public static class SensitiveWordChecker implements FlatMapFunction<String, Tuple3<String, String, String>> {
        private Set<String> sensitiveWords;

        public SensitiveWordChecker(Set<String> sensitiveWords) {
            this.sensitiveWords = sensitiveWords;
        }

        @Override
        public void flatMap(String value, Collector<Tuple3<String, String, String>> out) {
            try {
                String[] columns = value.split(",");
//                System.out.println("__"+columns.length+"__"+value);
                if (columns.length >= 3) {
                    String id = columns[0];
                    String content = columns[2];

                    for (String word : sensitiveWords) {
                        if (content.contains(word)) {
                            out.collect(new Tuple3<>("关键词检测", "微博ID："+id, "命中关键词："+word));
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
                String content = columns[2];

                // 分析情感
                String sentiment = analyzeSentiment(content);

                return new Tuple3<>("实时情感分析", "微博ID："+id, sentiment);
//                return new Tuple3<>("实时情感分析", "微博ID："+id, selectedWord);
            } else {
                return new Tuple3<>("实时情感分析", "", "内容无意义，无法分析");
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
    // 热度分析
    public static class TopicLikesExtractor implements FlatMapFunction<String, Tuple4<String, String, String,String>> {

        @Override
        public void flatMap(String value, Collector<Tuple4<String, String, String,String>> out) {
            String[] columns = value.split(",");
            if (columns.length >= 13) {
                try {
                    String id = columns[0];
                    int likes = Integer.parseInt(columns[9]);
                    int comments = Integer.parseInt(columns[10]);
                    int reposts = Integer.parseInt(columns[11]);
                    String topic = columns[12];
                    double total = (likes + comments*1.1 + reposts*1.3)/500;
                    if (total > 1) {
                        out.collect(new Tuple4<>("热度分析", "微博ID："+id, "话题："+topic,"热度指数："+ total));
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing numbers: " + e.getMessage());
                }
            }
        }

    }

}
