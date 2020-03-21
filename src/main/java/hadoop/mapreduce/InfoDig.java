package hadoop.mapreduce;

import hadoop.config.HBaseConfig;
import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author FancyKing
 */
public class InfoDig {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static int time = 0;

    public static class Map extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] childAndParent = line.split(" ");
            List<String> list = new ArrayList<String>(2);
            for (String childOrParent : childAndParent) {
                if (!"".equals(childOrParent)) {
                    list.add(childOrParent);
                }
            }
            if (!"child".equals(list.get(0))) {
                fastWriter.println("beh");
                String childName = list.get(0);
                String parentName = list.get(1);
                String relationType = "1";
                context.write(new Text(parentName), new Text(relationType + "+"
                        + childName + "+" + parentName));
                relationType = "2";
                context.write(new Text(childName), new Text(relationType + "+"
                        + childName + "+" + parentName));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //输出表头
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }

            //获取value-list中value的child
            List<String> grandChild = new ArrayList<String>();
            //获取value-list中value的parent
            List<String> grandParent = new ArrayList<String>();
            //左表，取出child放入grand_child
            for (Text text : values) {
                String s = text.toString();
                String[] relation = s.split("\\+");
                String relationType = relation[0];
                String childName = relation[1];
                String parentName = relation[2];
                if ("1".equals(relationType)) {
                    grandChild.add(childName);
                } else {
                    grandParent.add(parentName);
                }
            }

            //右表，取出parent放入grand_parent
            int grandParentNum = grandParent.size();
            int grandChildNum = grandChild.size();
            if (grandParentNum != 0 && grandChildNum != 0) {
                for (String s : grandChild) {
                    for (String value : grandParent) {
                        //输出结果
                        context.write(new Text(s), new Text(
                                value));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HadoopConfig.configuration;
        String[] otherArgs = new String[]{"/user/hadoop511/input", "/user/hadoop511/output"};
        Job job = Job.getInstance(conf, "Single table join");
        job.setJarByClass(InfoDig.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        fastWriter.println("echo");
    }

}
