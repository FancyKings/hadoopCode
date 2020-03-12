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
public class Makedir {

    static FastReader fastReader = HadoopConfig.fastReader;
    static FastWriter fastWriter = HadoopConfig.fastWriter;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

        if (args.length == 0) {
            fastWriter.println("Please the path you want to create:");
            args = fastReader.nextLine().split(HadoopConfig.splitChar);
        }
        String uri = args[0];
        fastWriter.print("You are creating path: " + uri + "\n"
                + "Status: ");
        FileSystem fileSystem = HadoopConfig.getFileSystem();
        Path toCreate = new Path(uri);

        // Cannot create a directory because a file already exists in a layer
//        String[] levelDir = uri.split(Config.slashChars);
//        int createStatus = isConflict(levelDir);

        fileSystem.mkdirs(toCreate);

        if (fileSystem.exists(toCreate)) {
            fastWriter.println("Success.");
        } else {
            fastWriter.println("Failed.");
        }
    }

    /**
     * @return If the node on the creation path conflicts with the existing file.
     *         Returns 1 otherwise 0
     */
    private static int isConflict(String[] levelDir) throws InterruptedException, IOException, URISyntaxException {

        if (levelDir.length == 0) {
            return 0;
        }
        FileSystem fileSystem = HadoopConfig.getFileSystem();
        StringBuilder checkingString = new StringBuilder();
        for (String subString : levelDir) {
            checkingString.append(subString);
            if (checkingString.charAt(checkingString.length()) != '/') {
                checkingString.append('/');
            }
            Path path = new Path(checkingString.toString());
            FileStatus[] fileStatus = fileSystem.listStatus(path);

        }
        return -9;
    }
}
