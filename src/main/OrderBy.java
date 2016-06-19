package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by root on 16-6-19.
 */

public class OrderBy {

    public static class TwoFieldKey implements WritableComparable<TwoFieldKey>{

        private Text company;
        private IntWritable orderNumber;

        public TwoFieldKey(){
            company = new Text();
            orderNumber = new IntWritable();
        }

        public TwoFieldKey(Text company,IntWritable orderNumber){
            this.company = company;
            this.orderNumber = orderNumber;
        }
        public Text getCompany(){
            return company;
        }
        public IntWritable getOrderNumber(){
            return orderNumber;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            company.readFields(in);
            orderNumber.readFields(in);
        }
//序列化与反序列化
        @Override
        public void write(DataOutput out) throws IOException {
            company.write(out);
            orderNumber.write(out);
        }

        @Override
        public int compareTo(TwoFieldKey other) {
            if(this.company.compareTo(other.getCompany()) == 0){
                return this.orderNumber.compareTo(other.getOrderNumber());//ORDER正序
            }else{
                return -this.company.compareTo(other.getCompany());//逆序
            }
        }

        @Override
        public boolean equals(Object right) {
            if(right instanceof TwoFieldKey){
                TwoFieldKey r = (TwoFieldKey) right;
                return r.getCompany().equals(company);//COMPANY相等就是相等的，MAP输出KEY
            }else{
                return false;
            }
        }

        @Override
        public String toString() {
            return "Company:" + company + "orderNumber:" + orderNumber;
        }
    }

    //WE CAN ALSO USE TotalOrderPartitioner
    public  static class PartByCompanyPartitioner extends
            Partitioner<TwoFieldKey,NullWritable>{
        @Override
        public int getPartition(TwoFieldKey key, NullWritable value, int numPartitions) {
            String company = key.getCompany().toString();//相同COMPANY出现在同一PARTITION


            int firstChar = (int) company.charAt(0);
            return (numPartitions-firstChar*numPartitions / 128-1);//COMPANY越大得到的值越小
        }
    }

    public static class mapper extends Mapper<Object, Text, TwoFieldKey, NullWritable> {

            public void map(Object key, Text value, Context context) throws IOException,
                    InterruptedException {
                String intputString = value.toString();
                String[] splits = intputString.split(" ");
                TwoFieldKey outKey = new TwoFieldKey(new Text(splits[0]),
                       new IntWritable(Integer.parseInt(splits[1])));
                context.write(outKey,NullWritable.get());
            }
    }


    public static class reducer extends Reducer<TwoFieldKey,NullWritable,Text,IntWritable> {

            public void reduce(TwoFieldKey key, Iterable<NullWritable> values,
                               Context context) throws IOException, InterruptedException {
                for (NullWritable val : values) {
                    Text company = key.getCompany();
                    IntWritable orderNumber = key.getOrderNumber();
                    context.write(company,orderNumber);
                }
            }
    }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            //这里需要配置参数即输入和输出的HDFS的文件路径
            if (otherArgs.length != 2) {
                System.err.println("Usage: wordcount <in> <out>");
                System.exit(2);
            }
            // JobConf conf1 = new JobConf(WordCount.class);
            Job job = new Job(conf, "homework");//Job(Configuration conf, String jobName) 设置job名称和
            job.setInputFormatClass(TextInputFormat.class);
            job.setJarByClass(OrderBy.class);//执行JAR时，找到类以及别的自定义类
            job.setMapperClass(mapper.class); //为job设置Mapper类
            job.setReducerClass(reducer.class); //为job设置Reduce类

            job.setMapOutputKeyClass(TwoFieldKey.class);
            job.setMapOutputValueClass(NullWritable.class);//表示不关心了

            job.setOutputKeyClass(Text.class);        //设置输出key的类型
            job.setOutputValueClass(IntWritable.class);//  设置输出value的类型

            job.setPartitionerClass(PartByCompanyPartitioner.class);//同一一个COMPANY记录在一个REDUCE

            job.setNumReduceTasks(5);//测试一下同一个COMPANY记录是否可以在同一个REDUCE出现，依赖于我们自定义的PARTITION
            FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径
            System.exit(job.waitForCompletion(true) ? 0 : 1);//调用waitForCompletion启动任务
        }
}
