package org.kiba.tools.job_001;


import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.kiba.enums.RecordState;
import org.kiba.models.Record;
import org.kiba.models.test_loginfo_new;
import org.kiba.utils.DBConnectionUtils;
import org.kiba.utils.SqlUtil;


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
    public void invoke(String json, Context context) {

        //"type":"read" "type":"insert" "type":"update" "type":"delete"
        log.info("===========json：" + json + "=========");
        Record record = JSON.parseObject(json, Record.class);
        String executeSql = "";

        if (record.getType().equals(RecordState.读取.getValue())) {
            executeSql = SqlUtil.getInsertSql("test_loginfo", test_loginfo_new.class, record.getAfter());

        } else if (record.getType().equals(RecordState.插入.getValue())) {
            executeSql = SqlUtil.getInsertSql("test_loginfo", test_loginfo_new.class, record.getAfter());

        } else if (record.getType().equals(RecordState.更新.getValue())) {
            executeSql = SqlUtil.getUpdateSql("test_loginfo", test_loginfo_new.class, record.getAfter());
        } else if (record.getType().equals(RecordState.删除.getValue())) {
            executeSql = SqlUtil.getDeleteSql("test_loginfo", record.getAfter());
        }
        if (!StrUtil.isBlankIfStr(executeSql)) {
            try {
                DBConnectionUtils du = DBConnectionUtils.GetInstance();
                Db db = DBConnectionUtils.GetInstance().getDb_middle();
                db.execute(executeSql);
            } catch (Exception e) {
                log.info("===========e-msg：" + e.getMessage() + "=========");
                log.info("===========record：" + record + "=========");
                log.info("===========excuteSql：" + executeSql + "=========");
                e.printStackTrace();
            }
        }

    }


    @Override
    public void close() throws Exception {
        super.close();
    }
}

