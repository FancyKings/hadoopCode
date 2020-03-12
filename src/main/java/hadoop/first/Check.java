package hadoop.first;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 */
public class Check {

    static FastWriter fastWriter = HadoopConfig.fastWriter;
    static FastReader fastReader = HadoopConfig.fastReader;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        String[] extraInput;
        if (args.length == 0) {

            fastWriter.println("Please enter the name of the file you want to retrieval:");
            extraInput = fastReader.nextLine().split(HadoopConfig.splitChar);
        } else {
            extraInput = args;
        }
        fastWriter.println("We are retrieving the file directory.");
        boolean flagExist = isExist(extraInput);

        fastWriter.print("File retrieval status is: ");
        if (!flagExist) {
//            fastWriter.println("NO File.");
//            WriteFile.main(args);
            fastWriter.println("New file created");
        } else {
            fastWriter.println("Have File.");
        }
    }

    public static boolean isExist(String[] fileNames) throws IOException, URISyntaxException, InterruptedException {

        return false;
//        FileSystem fileSystem = Config.getFileSystem();
//        int flagAllExist = 0;
//
//        for (String fileName : fileNames) {
//
//            if (fileSystem.exists(new Path(fileName))) {
//                flagAllExist += 1;
//            }
//        }
//        return (flagAllExist == fileNames.length);
    }
}
