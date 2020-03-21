package hadoop.mapreduce;

import hadoop.config.HadoopConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class test {

    /**
     * @param args
     * 对A,B两个文件进行合并，并剔除其中重复的内容，得到一个新的输出文件C
     */
    //在这重载map函数，直接将输入中的value复制到输出数据的key上 注意在map方法中要抛出异常：throws IOException,InterruptedException

    /********** Begin **********/
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text text = new Text();
        @Override
        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {

//            value.append("AA".getBytes(), 0, "AA".getBytes().length);
            text = value;
            content.write(text, new Text(""));
        }
    }


    /********** End **********/




    //在这重载reduce函数，直接将输入中的key复制到输出数据的key上  注意在reduce方法上要抛出异常：throws IOException,InterruptedException
    /********** Begin **********/

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            key.append("CC".getBytes(), 0, "CC".getBytes().length);
            context.write(key, new Text(""));
        }
    }

    /********** End **********/






    public static void main(String[] args) throws Exception{

        // TODO Auto-generated method stub
        Configuration conf = HadoopConfig.configuration;

        Job job = Job.getInstance(conf,"Merge and duplicate removal");
        job.setJarByClass(test.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputPath = "/user/hadoop511/input";  //在这里设置输入路径
        String outputPath = "/user/hadoop511/output";  //在这里设置输出路径

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
