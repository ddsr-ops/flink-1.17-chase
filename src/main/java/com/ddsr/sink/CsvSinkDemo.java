package com.ddsr.sink;

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
        File file = new File(csvPath);
        if (file.exists()) {
            boolean delete = file.delete();
            System.out.println("deleted ? = " + delete);
        }


        // Writes tuples as comma-separated value files. Row and field delimiters are configurable. The value for
        // each field comes from the toString() method of the objects.
        env.fromCollection(staffList)
                .writeAsText(csvPath);
        env.execute();
    }
}
