package hadoop.second;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author FancyKing
 */
public class List {

    static FastReader fastReader = HadoopConfig.fastReader;
    static FastWriter fastWriter = HadoopConfig.fastWriter;

    private static int depth = 0;
    private static int maxDepth = Integer.MAX_VALUE;
    private static int fileCnt = 0;
    private static int dirCnt = 0;


    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

        args = readArgs(args);
        ls(args);
        fastWriter.println("total files: " + fileCnt);
        fastWriter.println("total directory: " + dirCnt);
    }

    private static void ls(String[] args) throws InterruptedException, IOException, URISyntaxException {

        String path = args[0];
        FileStatus[] fileStatus = HadoopConfig.getFileSystem().listStatus(
                new Path(path)
        );
        for (FileStatus file : fileStatus) {
            if (file.isFile()) {
                fastWriter.print(getSpace());
                fastWriter.println(
                        "[" + getName(path, file) + "] " +
                                "[" + getMod(file) + "] " +
                                "[" + getSize(file) + "] " +
                                "[" + unixTimeToDate(getCreateDate(file)) + "] "
                );
                ++fileCnt;
            }
            if (file.isDirectory()) {
                String dirName = getName(path, file);
                String subPath = addSlash(path) + dirName;
                fastWriter.println(getSpace() + dirName + '/');
                depth += 2;
                ls(subPath.split(HadoopConfig.splitChar));
                depth -= 2;
                ++dirCnt;
            }
        }
    }

    private static long getCreateDate(FileStatus tot) {
        return tot.getAccessTime();
    }

    private static FsPermission getMod(FileStatus tot) {
        return tot.getPermission();
    }

    public static String getName(String path, FileStatus tot) {
        return (tot.getPath().toString().
                replaceAll(HadoopConfig.hdfsPath + addSlash(path), ""));
    }

    private static String getOwner(FileStatus tot) {
        return (tot.getOwner());
    }

    private static String getSize(FileStatus tot) {
        long size = tot.getLen();
        int k = 1024;
        int m = k * 1024;
        int g = m * 1024;
        if (size / g > 0) {
            return size / g + "GB";
        } else if (size / m > 0) {
            return size / m + "MB";
        } else if (size / k > 0) {
            return size / k + "KB";
        } else {
            return size + "B";
        }
    }

    public static String addSlash(String ori) {
        return (ori.charAt(ori.length() - 1) == HadoopConfig.slashChar)
                ? (ori) : (ori + HadoopConfig.slashChar);
    }

    private static String getSpace() {
        StringBuilder ans = new StringBuilder();
        int num = depth;
        if (depth != 0) {
            ans.append("|");
        }
        while (num >= 1) {
            ans.append('-');
            num -= 1;
            if (num % 2 == 0 && num != 0) {
                ans.append("|");
            }
        }
        return ans.toString();
    }

    private static String unixTimeToDate(Long unixTime) {
        return new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss",
                Locale.CHINA
        ).format(new Date(unixTime));
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
