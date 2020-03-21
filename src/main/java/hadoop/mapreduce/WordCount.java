package hadoop.mapreduce;


import hadoop.config.HBaseConfig;
import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Teacher
 */
public class WordCount {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        //单词个数定义为1
        private final static IntWritable one = new IntWritable(1);
        //单词word
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("The input of Map: <" + key.toString() + "," + value + ">");

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                //解析出的每个单词放入变量word
                word.set(itr.nextToken());
                //输出<单词，个数>
                context.write(word, one);
                System.out.println("The out of Map: <" + word.toString() + "," + one + ">");
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //相同的单词，个数进行相加
            int sum = 0;
            StringBuilder list = new StringBuilder("<");
            for (IntWritable val : values) {
                sum += val.get();
                list.append(val).append(",");
            }
            int len = list.length();
            list = new StringBuilder(list.substring(0, len - 1));
            list.append(">");
            System.out.println("The input of Reduce: <" + key.toString() + "," + list + ">");

            result.set(sum);
            //输出<单词，总个数>
            context.write(key, result);
            System.out.println("The out of Reduce: <" + key.toString() + "," + result + ">");
        }
    }

    public static void main(String[] args) throws Exception {
        //设置用户名
        System.setProperty("HADOOP_USER_NAME", "hadoop511");
        Configuration conf = HadoopConfig.configuration;

        FileSystem fs = FileSystem.get(conf);


        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/user/hadoop511/input/file1"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hadoop511/output"));
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
