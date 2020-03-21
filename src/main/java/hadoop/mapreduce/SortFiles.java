package hadoop.mapreduce;

import hadoop.config.HadoopConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class SortFiles {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {

        private static IntWritable data = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /********** Begin **********/
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, new IntWritable(1));
            /********** End **********/

        }
    }

    //reduce函数将map输入的key复制到输出的value上，然后根据输入的value-list中元素的个数决定key的输出次数,定义一个全局变量line_num来代表key的位次
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private static IntWritable line_num = new IntWritable(1);

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /********** Begin **********/
            for (IntWritable num : values) {
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() + 1);
            }
            /********** End **********/
        }
    }

    //自定义Partition函数，此函数根据输入数据的最大值和MapReduce框架中Partition的数量获取将输入数据按照大小分块的边界，然后根据输入数值和边界的关系返回对应的Partiton ID
    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartition) {
            /********** Begin **********/
            int maxnumber = 65223;//int型的最大数值
            int bound = maxnumber / numPartition + 1;
            int keynumber = key.get();
            for (int i = 0; i < numPartition; i++) {
                if (keynumber < bound * i && keynumber >= bound * (i - 1)) {
                    return i - 1;
                }
            }
            return -1;
            /********** End **********/

        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop511");
        Configuration conf = HadoopConfig.configuration;
        String[] otherArgs = new String[]{"input/", "output"};
        Job job = Job.getInstance(conf, "Merge and Sort");
        job.setJarByClass(SortFiles.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
