package com.ddsr.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * @author ddsr, created it at 2024/11/5 18:14
 */
@SuppressWarnings("deprecation")
public class CsvSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        List<Staff> staffList = Arrays.asList(new Staff("张三", 30), new Staff("李四", 40), new Staff("王五", 50));


        String csvPath = "D:\\JavaWorkspaceIJ\\Study\\Flink\\flink-1.17-chase\\out\\Staff.csv";

        // If the file already exists, it would be removed first
//        clearFileIfExist(csvPath);


        // Writes tuples as comma-separated value files. Row and field delimiters are configurable. The value for
        // each field comes from the toString() method of the objects.
        env.fromCollection(staffList)
//                .writeAsText(csvPath);
                // overwrite mode
                .writeAsText(csvPath, FileSystem.WriteMode.OVERWRITE);


        String csvPath1 = "D:\\JavaWorkspaceIJ\\Study\\Flink\\flink-1.17-chase\\out\\Staff1.csv";
        clearFileIfExist(csvPath1);
        env.fromCollection(Arrays.asList(
                        new Tuple2<>("张三", "2"), new Tuple2<>("李四", "4"),
                        new Tuple2<>("王五", "6"), new Tuple2<>("赵六", "8")
                ))
                .writeAsText(csvPath1);
        env.execute();
    }


    private static void clearFileIfExist(String path) {
        File file = new File(path);
        if (file.exists()) {
            boolean delete = file.delete();
            System.out.println("deleted ? = " + delete);
        }
    }
}
