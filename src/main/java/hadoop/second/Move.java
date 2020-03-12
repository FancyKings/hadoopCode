package hadoop.second;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 */
public class Move {

    static FastReader fastReader = HadoopConfig.fastReader;
    static FastWriter fastWriter = HadoopConfig.fastWriter;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        args = readArgs(args);
        if (!detectSrc(args[0])) {
            fastWriter.println("The source file does not exist");
        } else {
            mvFile(args);
        }
    }

    private static void mvFile(String[] args) throws InterruptedException, IOException, URISyntaxException {

        FileSystem fileSystem = HadoopConfig.getFileSystem();
        fileSystem.rename(
                new Path(args[0]),
                new Path(args[1])
        );
        if (!fileSystem.exists(new Path(args[0])) && fileSystem.exists(new Path(args[1]))) {
            fastWriter.println("Move successfully");
        } else {
            fastWriter.println("Move failed");
        }
        fileSystem.close();
    }

    private static boolean detectSrc(String src) throws InterruptedException, IOException, URISyntaxException {

        FileSystem fileSystem = HadoopConfig.getFileSystem();
        return fileSystem.exists(new Path(src));
    }

    private static String[] readArgs(String[] args) throws IOException {

        if (args.length < 2) {
            String[] ans = new String[2];
            fastWriter.println("Input error, please also specify input and output paths");
            fastWriter.println("The input path is:");
            ans[0] = fastReader.nextLine();
            fastWriter.println("The output path is:");
            ans[1] = fastReader.nextLine();
            args = ans;
        }
        return args;
    }
}
