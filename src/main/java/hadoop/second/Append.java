package hadoop.second;

import hadoop.config.HadoopConfig;
import hadoop.config.RandomString;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import hadoop.first.Read;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 * args[0] : head / tail
 * args[1] : file path
 * args[2] : Append text
 */
public class Append {

    static FastWriter fastWriter = HadoopConfig.fastWriter;
    static FastReader fastReader = HadoopConfig.fastReader;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        args = readArgs(args);
        if ("tail".equals(args[0])) {
            tailAppend(args[1], args[2]);
        }
    }

    private static void tailAppend(String dir, String cont) throws InterruptedException, IOException, URISyntaxException {

        FileSystem fileSystem = HadoopConfig.getFileSystem();
        // Detect HDFS configuration file APPEND settings
        boolean settingStatus = Boolean.getBoolean(
                fileSystem.getConf().get("dfs.support.append")
        );
        if (settingStatus) {
            FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path(dir));
            PrintWriter printWriter = new PrintWriter(fsDataOutputStream);
            printWriter.append(cont);
            printWriter.flush();
            printWriter.close();
            fastWriter.println("Append Success");
            Read.main(dir.split(HadoopConfig.splitChar));
        } else {

            fastWriter.println("API is disabled, enable alternatives");

            // Read before write
            fastWriter.print("Append Status: ");
            boolean accessTailStatus = appendToTail(dir, cont);
            if (accessTailStatus) {
                fastWriter.println("Success.");
            } else {
                fastWriter.println("Fail.");
            }
        }
        fileSystem.close();
    }

    private static boolean appendToTail(String dir, String cont) throws InterruptedException, IOException, URISyntaxException {

        String tmpString = RandomString.getString(10);
        FileSystem fileSystem = HadoopConfig.getFileSystem();
        if (!fileSystem.exists(new Path(dir))) {
            return false;
        }
        FSDataInputStream dirStream = fileSystem.open(new Path(dir));
        FSDataOutputStream tmpStream = fileSystem.create(new Path(tmpString));

        String dirString = IOUtils.toString(dirStream, "UTF-8");
        tmpStream.writeBytes(dirString + "\n");
        tmpStream.writeBytes(cont);
        fileSystem.delete(new Path(dir), false);
        fileSystem.rename(new Path(tmpString), new Path(dir));
        return fileSystem.exists(new Path(dir));
    }

    private static String[] readArgs(String[] args) throws IOException {

        if (args.length < 3) {
            String[] ans = new String[3];
            fastWriter.println("Please enter additional mode [head / tail]");
            ans[0] = fastReader.nextLine();
            fastWriter.println("Please enter additional file address");
            ans[1] = fastReader.nextLine();
            fastWriter.println("Please enter additional text");
            ans[2] = fastReader.nextLine();
            args = ans;
        }
        return args;
    }
}
