package hadoop.second;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 */
public class Remove {

    static FastReader fastReader = HadoopConfig.fastReader;
    static FastWriter fastWriter = HadoopConfig.fastWriter;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        args = readArgs(args);
        for (String subFile : args) {
            int removeStatus = remove(subFile);
            if (removeStatus == 0) {
                fastWriter.println("Success.");
            } else if (removeStatus == 1) {
                // Action abnormal when top-level folder is empty
                fastWriter.println("Success.");
            } else {
                fastWriter.println("No detect");
            }
        }
    }

    private static int remove(String file) throws InterruptedException, IOException, URISyntaxException {

        Path path = new Path(file);
        FileSystem fileSystem = HadoopConfig.getFileSystem();

        if (!fileSystem.exists(path)) {
            return -1;
        }

        int deleteFilesCnt = 0;
        FileStatus[] fileStatus = fileSystem.listStatus(path);
        // No files and subdirectories in the current path
        if (fileStatus.length == 0) {
            fileSystem.delete(path, false);
            return 1;
        }
        // Recursively check files and subdirectories
        for (FileStatus subSileStatus : fileStatus) {

            String subDirUri = subSileStatus.getPath().toString();
            if (subSileStatus.isDirectory()) {
                int delayFile;
                if (file.charAt(file.length() - 1) == '/') {
                    delayFile = remove(file +
                            subDirUri.substring(subDirUri.lastIndexOf('/') + 1));
                } else {
                    delayFile = remove(file +
                            subDirUri.substring(subDirUri.lastIndexOf('/')));
                }
                if (delayFile == 0) {
                    fastWriter.println("sub have file, no delete cur dir");
                } else {
                    fastWriter.println("sub clean, delete cur dir");
                    fileSystem.delete(new Path(subDirUri), false);
                }
            } else {
                fastWriter.println("Do you want to delete[y/n]:" + subSileStatus.getPath());
                String deleteAnswer = fastReader.nextLine();
                if ("y".equals(deleteAnswer) || "Y".equals(deleteAnswer)) {
                    fastWriter.println("file deleted " + subDirUri);
                    fileSystem.delete(new Path(subDirUri), false);
                    ++deleteFilesCnt;
                }
            }
        }

        if (deleteFilesCnt == fileStatus.length) {
            return 1;
        } else {
            return 0;
        }
    }

    private static String[] readArgs(String[] args) throws IOException {

        if (args.length == 0) {
            fastWriter.println("input:");
            return fastReader.nextLine().split(HadoopConfig.splitChar);
        } else {
            return args;
        }
    }
}
