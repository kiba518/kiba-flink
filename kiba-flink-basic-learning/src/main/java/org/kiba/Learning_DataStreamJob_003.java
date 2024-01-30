package org.kiba;

import cn.hutool.core.date.DateUtil;
import com.esotericsoftware.minlog.Log;
import com.mongodb.client.*;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.*;
import org.kiba.tools.job002.MySQTest_LogInfoSource;
import org.kiba.tools.job002.MySqlTest_LogInfo_ResultWriter;
import org.kiba.tools.job002.Test_LogInfo;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 利用mangodb-cdc的库，连接mangoDb读取数据，并未实现cdc
 */
public class Learning_DataStreamJob_003 {

    public static void main(String[] args) throws Exception {

        //1.创建链接
        MongoClient client = MongoClients.create("mongodb://10.1.100.22:27017");//27017
        //2.打开数据库test
        MongoDatabase db = client.getDatabase("blade-log");
        //3.获取集合
        MongoCollection<Document> collection = db.getCollection("blade_log_api");
        BsonDocument bd = new BsonDocument("$group", new BsonArray(Arrays.asList(
                new BsonDocument("_id", new BsonDocument("createBy", new BsonString("$createBy")))
        )));
        // 执行 aggregate 操作
        Log.info(bd.toString());
        BsonArray ba = new BsonArray(Arrays.asList(
                //new BsonDocument("$match",new BsonDocument("createBy",new BsonDocument("$exists",new BsonBoolean(true))))
                BsonDocument.parse("{ $match: { createBy: { $exists: true } } }"),
                BsonDocument.parse("{ $sort : { createTime :-1 }}"),//1：升序，-1：降序

                new BsonDocument("$group", new BsonArray(Arrays.asList(
                        new BsonDocument("_id", new BsonDocument("createBy", new BsonString("$createBy")))
                ))),
                BsonDocument.parse("{ $limit: 10 }")
        ));
        Log.info(
                Arrays.asList(
                        //new BsonDocument("$match",new BsonDocument("createBy",new BsonDocument("$exists",new BsonBoolean(true))))
                        new BsonDocument("$match", new BsonDocument(Arrays.asList(
                                new BsonElement("createBy", new BsonDocument("$exists", new BsonBoolean(true))),
                                new BsonElement("createTime", new BsonDocument("$gt", new BsonDateTime(DateUtil.parse(DateUtil.now()).getTime())))
                        ))),
                        //BsonDocument.parse("{ $match: { createBy: { $exists: true },createTime: { $gt: new ISODate('2023-11-28 06:11:00.222') } } }"),
                        //BsonDocument.parse("{ _id: { $gt: 0 } }"),
                        new BsonDocument("$group",
                                new BsonDocument(
                                        Arrays.asList(
                                                new BsonElement("_id",
                                                        new BsonDocument(Arrays.asList(
                                                                new BsonElement("createBy", new BsonString("$createBy")),
                                                                new BsonElement("createTime", new BsonDocument("$substr",
                                                                        new BsonArray(Arrays.asList(new BsonString("$createTime"), new BsonInt32(0), new BsonInt32(10)))
                                                                ))
                                                        ))
                                                ),
                                                new BsonElement("remoteIp", new BsonDocument("$last", new BsonString("$remoteIp"))),
                                                new BsonElement("userAgent", new BsonDocument("$last", new BsonString("$userAgent"))),
                                                new BsonElement("tenantId", new BsonDocument("$last", new BsonString("$tenantId"))),
                                                new BsonElement("createTime", new BsonDocument("$max", new BsonString("$createTime")))
                                                //new BsonElement("createTime", new BsonDocument("$last", new BsonString("$createTime")))
                                        )

                                )


                        ),
                        BsonDocument.parse("{ $sort : { createTime : 1 }}")//1：升序，-1：降序

                        //BsonDocument.parse("{ $limit: 10 }")


                ).toString());


//        Arrays.asList(new BsonElement("_id", new BsonDocument("createBy", new BsonString("$createBy"))),
//                new BsonElement("remoteIp", new BsonDocument("$last", new BsonString("$remoteIp"))));
//        List<BsonElement> bdList = new ArrayList<>();
//        bdList.add(new BsonElement("_id", new BsonDocument("createBy", new BsonString("$createBy"))));
//        bdList.add(new BsonElement("remoteIp", new BsonDocument("$last", new BsonString("$remoteIp"))));
//        BsonDocument  bd1 = new BsonDocument(bdList);
        //{ $match: { createTime: { $gt: new ISODate("2023-11-28 06:11:00.222") } } }
        MongoCursor<Document> alist = collection.aggregate(Arrays.asList(
                //new BsonDocument("$match",new BsonDocument("createBy",new BsonDocument("$exists",new BsonBoolean(true))))
                new BsonDocument("$match", new BsonDocument(Arrays.asList(
                        new BsonElement("createBy", new BsonDocument("$exists", new BsonBoolean(true))),
                        new BsonElement("createTime", new BsonDocument("$gt", new BsonDateTime(DateUtil.parse(DateUtil.now()).getTime())))
                ))),
                //BsonDocument.parse("{ $match: { createBy: { $exists: true },createTime: { $gt: new ISODate('2023-11-28 06:11:00.222') } } }"),
                //BsonDocument.parse("{ _id: { $gt: 0 } }"),
                new BsonDocument("$group",
                        new BsonDocument(
                                Arrays.asList(
                                        new BsonElement("_id",
                                                new BsonDocument(Arrays.asList(
                                                        new BsonElement("createBy", new BsonString("$createBy")),
                                                        new BsonElement("createTime", new BsonDocument("$substr",
                                                                new BsonArray(Arrays.asList(new BsonString("$createTime"), new BsonInt32(0), new BsonInt32(10)))
                                                        ))
                                                ))
                                        ),
                                        new BsonElement("remoteIp", new BsonDocument("$last", new BsonString("$remoteIp"))),
                                        new BsonElement("userAgent", new BsonDocument("$last", new BsonString("$userAgent"))),
                                        new BsonElement("tenantId", new BsonDocument("$last", new BsonString("$tenantId"))),
                                        new BsonElement("createTime", new BsonDocument("$max", new BsonString("$createTime")))
                                        //new BsonElement("createTime", new BsonDocument("$last", new BsonString("$createTime")))
                                )

                        )


                ),
                BsonDocument.parse("{ $sort : { createTime : 1 }}")//1：升序，-1：降序

                //BsonDocument.parse("{ $limit: 10 }")
        )).iterator();

        while (alist.hasNext()) {
            Document document = alist.next();
            Date createTime = DateUtil.parse(document.get("createTime").toString());
            String _id = document.get("_id").toString();
            Log.info(_id);
            Log.info(DateUtil.formatDateTime(createTime));
            System.out.println(document);
        }


        //4.查询获取文档集合
//        //4.构建查询条件，按照name来查询
//        BasicDBObject stu = new BasicDBObject("_id", 1641645005988913152l);
//        FindIterable<Document> documents = collection.find(stu);
//
//
//        //5.循环遍历
//        for (Document document : documents) {
//            System.out.println(document);
//        }

//        Bson command =   BsonDocument.parse("blade_log_api.count();");
//        Document result = db.runCommand(command);
//
//        //打印结果
//        System.out.println(result.toJson());
        //6.关闭连接
        client.close();
    }






}
