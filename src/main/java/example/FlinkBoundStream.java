package example;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlinkBoundStream {
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

        // 3. 获取源数据
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");

        // 4. 数据处理
        dataStreamSource
                // (1) 一对多：将一行数据映射为多个单词，
                //     同时做一对一映射转换：将单词 word 映射为 (word, 1) 形式。
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        //  1) 对该行数据切割为一个一个的单词
                        String[] words = line.split(" ");
                        //  2) 将单词以元组(word, 1)的形式加入到收集器中
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                // (3) 分组统计
                // 后面的String是打上的标签类型，后面会根据标签分组。
                // 因为这里是根据tuple2.f0字段分组，所以标签类型和tuple2.f0类型相同
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() { // DataStream API 的算子
                    @Override
                    public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                        return tuple2.f0;  // f0是指元组的0下标字段
                    }
                })
                .sum(1)
                .print();

        // 5. 执行(流数据处理必须有这一步)
        env.execute("BoundStream"); // 传入的字符串是设置该job的名字，在WebUI界面会显示
    }
}
