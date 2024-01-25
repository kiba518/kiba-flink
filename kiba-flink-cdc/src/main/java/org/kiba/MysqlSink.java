package org.kiba;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MysqlSink extends RichSinkFunction<String> {

    private PreparedStatement psInsert = null;
    private PreparedStatement psUpdate = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void invoke(String list, Context context) {
            log.info(list);
        //"type":"read" "type":"insert" "type":"update" "type":"delete"
    }



    @Override
    public void close() throws Exception {
        super.close();
    }
}

