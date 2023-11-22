package com.ddsr.iceberg;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;

/**
 * @author ddsr, created it at 2023/11/22 10:32
 */
public class IcebergReading {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");

        IcebergSource<RowData> source = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .assignerFactory(new SimpleSplitAssignerFactory())
                .build();

        DataStream<RowData> batch = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "My Iceberg Source",
                TypeInformation.of(RowData.class));

        // Print all records to stdout.
        batch.print();

        env.execute();
    }
}
