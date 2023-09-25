package com.ddsr.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Never recommend to define a sink by ourselves
 *
 * @author ddsr, created it at 2023/8/20 16:15
 */
public class CustomSinkDemo {
    public class MySink extends RichSinkFunction<String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialization
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            // Destroy something
            super.close();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            // output data
        }
    }
}
