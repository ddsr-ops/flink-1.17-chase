package com.ddsr.state.examples.queryable;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/**
 * @author ddsr, created it at 2024/12/17 18:25
 */
@SuppressWarnings("CallToPrintStackTrace")
public class QueryStateDemo {
    public static void main(String[] args) throws UnknownHostException {


        // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/queryable_state/#proxy
        // where is the remotePort from?
        QueryableStateClient client = new QueryableStateClient("tmHostname", 9069);

    // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }));

        // JobId from flink command or flink UI
        JobID jobId = JobID.fromHexString("fd72014d4c864993a2e5a9287b4a9c5d");
        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
                client.getKvState(jobId, "current-cnt-avg", 1L, BasicTypeInfo.LONG_TYPE_INFO, descriptor);

    // now handle the returned value
        resultFuture.thenAccept(response -> {
            try {
                Tuple2<Long, Long> res = response.value();
                System.out.println(res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }
}
