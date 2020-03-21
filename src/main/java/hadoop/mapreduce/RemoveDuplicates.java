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
public class RemoveDuplicates {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {

            content.write(value, new Text(""));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            key.append("CC".getBytes(), 0, "CC".getBytes().length);
            context.write(key, new Text(""));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = HadoopConfig.configuration;

        Job job = Job.getInstance(conf, "Merge and duplicate removal");
        job.setJarByClass(test.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputPath = "/user/hadoop511/input";
        String outputPath = "/user/hadoop511/output";

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
