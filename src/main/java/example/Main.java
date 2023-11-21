package example;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}
//public class FlinkWorldCount {
//    public static void main(String[] args) throws Exception {
//        //创建环境
//        StreamExecutionEnvironment env1=StreamExecutionEnvironment.getExecutionEnvironment();
//        LocalStreamEnvironment env2=StreamExecutionEnvironment.createLocalEnvironment();
//        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        //读取文件
//        DataSource<String>dataSource=env.readTextFile("./data/words");
//        FlatMapOperator<String,String>words=dataSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String line, Collector<String> out) throws Exception {
//                String []split=line.split(" ");
//                for (String word : split) {
//                    out.collect(word);
//                }
//            }
//        });
//        MapOperator<String,Tuple2<String,Integer>>map=words.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2 <String,Integer>map(String word) throws Exception {
//                return new Tuple2<>(word,1);
//            }
//        });
//        UnsortedGrouping<Tuple2<String,Integer>>grouping=map.groupBy(0);
//        DataSet<Tuple2<String,Integer>>sum=grouping.sum(1);
//        SortPartitionOperator<Tuple2<String,Integer>>result=sum.sortPartition(1, Order.DESCENDING);
//        //打印运行结果
//        sum.print();
//        // 将运行结果写入到文件中
//        //  DataSink<Tuple2<String,Integer>>tuple2DataSink=result.writeAsCsv("./data/reuslt",";","=",  		               FileSystem.WriteMode.OVERWRITE);
//        //   env.execute();
//    }
//}
