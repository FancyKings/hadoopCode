package hadoop.first;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

/**
 * @author FancyKing
 */
public class Read {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        FastReader fastReader = HadoopConfig.fastReader;
        FastWriter fastWriter = HadoopConfig.fastWriter;

        fastWriter.println("Please enter the name of the document you want to view:");
        String fileName = fastReader.nextLine();

        if (!Check.isExist(fileName.split(HadoopConfig.splitChar))) {

            fastWriter.println("The file name you entered does not exist.");
        } else {

            FileSystem fileSystem = HadoopConfig.getFileSystem();
            FSDataInputStream fsDataInputStream = fileSystem.open(
                    new Path(fileName)
            );
            BufferedReader fileReader = new BufferedReader(
                    new InputStreamReader(fsDataInputStream)
            );

            fastWriter.println("Content of the file are shown below:"
                    + "\n===>");
            fastWriter.println(fileReader.readLine());
            fastWriter.println("<===\n");
            fileReader.close();
        }
    }
}
