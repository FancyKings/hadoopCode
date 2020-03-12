package hadoop.second;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 */
public class WriteFile {

    static FastWriter fastWriter = HadoopConfig.fastWriter;
    static FastReader fastReader = HadoopConfig.fastReader;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        args = readArgs(args);
        String subFile = args[0];
        Path path = new Path(subFile);
        int checkStatus = checkExistAndRewrite(path);
        if (checkStatus == 0) {
            fastWriter.println("New file created");
        } else if (checkStatus == 1) {
            fastWriter.println("Rewrite old files");
        } else {
            fastWriter.println("Operation terminated");
            return;
        }
        writeToPath(subFile, path, args[1]);
    }

    private static void writeToPath(String fileUri, Path path, String text) throws InterruptedException, IOException, URISyntaxException {

        FileSystem fileSystem = HadoopConfig.getFileSystem();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        byte[] typeInByte = text.getBytes();
        fastWriter.print("You are writing below data to file ["
                + fileUri.substring(fileUri.lastIndexOf('/') + 1) + "]\n" +
                "===>\n" + text + "\n<===\n"
                + "and status is: ");
        fsDataOutputStream.write(typeInByte, 0, typeInByte.length);
        fsDataOutputStream.hflush();
        fsDataOutputStream.close();

        if (fileSystem.exists(path)) {
            fastWriter.println("Success.");
        } else {
            fastWriter.println("Failed.");
        }
        fileSystem.close();
    }

    private static int checkExistAndRewrite(Path path) throws IOException, URISyntaxException, InterruptedException {

        FileSystem fileSystem = HadoopConfig.getFileSystem();
        if (fileSystem.exists(path)) {
            fastWriter.println("File already exists, discard old content and rewrite?[y/n]");
            // cat file some line
            String rewriteAnswer = fastReader.nextLine();
            if ("y".equals(rewriteAnswer) || "Y".equals(rewriteAnswer)) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }

    private static String[] readArgs(String[] args) throws IOException {

        // Assume interactive input and output temporarily
        String[] ans = new String[2];
        if (args.length < 2) {
            fastWriter.println("input path:");
            ans[0] = fastReader.nextLine();
            fastWriter.println("input data:");
            ans[1] = fastReader.nextLine();
            args = ans;
        }
        return args;
    }
}
