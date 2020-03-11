package hadoop.first;

import hadoop.config.Config;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * The code status is old
 * @author FancyKing
 */
public class Write {

    static FastWriter fastWriter = Config.fastWriter;
    static FastReader fastReader = Config.fastReader;

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {


        fastWriter.println("The name of the file you want to create:");
        String fileName = fastReader.nextLine();
        fastWriter.println("Please enter what you want to write:");
        String typeIn = fastReader.nextLine();
        byte[] typeInByte = typeIn.getBytes();
        fastWriter.print("You are writing below data to file "
                + fileName + "\n" +
                "===>\n" + typeIn + "\n<===\n"
                + "and status is: ");

        FileSystem fileSystem = Config.getFileSystem();
        FSDataOutputStream fsDataOutputStream = fileSystem.create(
                new Path(fileName)
        );
        fsDataOutputStream.write(typeInByte, 0, typeInByte.length);

        String[] checkList = fileName.split(Config.splitChar);
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        if (!Check.isExist(checkList)) {
            fastWriter.println("Success.");
        } else {
            fastWriter.println("Failed.");
        }
    }
}
