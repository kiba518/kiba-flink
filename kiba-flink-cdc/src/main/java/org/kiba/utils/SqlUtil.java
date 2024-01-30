package org.kiba.utils;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ReflectUtil;
import cn.hutool.core.util.StrUtil;

import java.lang.reflect.Field;
import java.util.Date;

public class SqlUtil {
    /**
     * 生成插入语句
     * @param tablename 表名
     * @param clazz 与数据库中字段一一对应的类
     * @param t 有数据的实体
     * @param <T> 数据实体类型 如 User
     */
    public static  <T> String getInsertSql(String tablename, Class<T> clazz, T t){
        //insert into table_name (column_name1,column_name2, ...) values (value1,value2, ...)
        String sql = "";
        Field[] fields = ReflectUtil.getFieldsDirectly(clazz, false);
        StringBuffer topHalf = new StringBuffer("insert into "+tablename+" (");
        StringBuffer afterAalf = new StringBuffer("values (");
        for (Field field : fields) {
//            if ("ID".equals(field.getName()) || "id".equals(field.getName())){
//                continue;   //id 自动生成无需手动插入
//            }
            topHalf.append(field.getName() + ",");
            if (ReflectUtil.getFieldValue(t, field.getName()) instanceof String) {
                afterAalf.append("'" +StrUtil.replace(Convert.toStr(ReflectUtil.getFieldValue(t, field.getName())) ,"'","''")+ "',");
            }
            else if (ReflectUtil.getFieldValue(t, field.getName()) instanceof Date) {
                afterAalf.append("'" + DateUtil.formatDateTime(Convert.toDate((ReflectUtil.getFieldValue(t, field.getName())))) + "',");
            }
            else {
                afterAalf.append(ReflectUtil.getFieldValue(t, field.getName()) + ",");
            }
        }
        topHalf = new StringBuffer(StrUtil.removeSuffix(topHalf.toString(), ","));
        afterAalf = new StringBuffer(StrUtil.removeSuffix(afterAalf.toString(), ","));
        topHalf.append(") ");
        afterAalf.append(") ");
        sql = topHalf.toString() + afterAalf.toString();
        return sql;
    }

    /**
     * 生成更新语句
     * 必须含有id
     * 数据实体中 null 与 空字段不参与更新
     * @param tablename 数据库中的表明
     * @param clazz 与数据库中字段一一对应的类
     * @param t 有数据的实体
     * @param <T> 数据实体类型,如 User
     */
    public static  <T> String getUpdateSql(String tablename, Class<T> clazz, T t){
        //UPDATE table_name SET column_name1 = value1, column_name2 = value2, ... where ID=xxx
        //or
        //UPDATE table_name SET column_name1 = value1, column_name2 = value2, ... where id=xxx
        String sql = "";
        String id = ""; //保存id名：ID or id
        Field[] fields = ReflectUtil.getFieldsDirectly(clazz, false);
        sql = "update "+tablename+" set ";
        for (Field field : fields) {
            StringBuffer tmp = new StringBuffer();
            if ("ID".equals(field.getName()) || "id".equals(field.getName())){
                id = field.getName();
                continue;//更新的时候无需set id=xxx
            }
            if (ReflectUtil.getFieldValue(t, field.getName()) != null && ReflectUtil.getFieldValue(t, field.getName()) != "") {
                tmp.append( field.getName() + "=");
                if (ReflectUtil.getFieldValue(t, field.getName()) instanceof String) {
                    tmp.append("'" +StrUtil.replace(Convert.toStr(ReflectUtil.getFieldValue(t, field.getName())) ,"'","''")+ "',");
                    //tmp.append( "'" + ReflectUtil.getFieldValue(t, field.getName()) + "',");
                }
                else if (ReflectUtil.getFieldValue(t, field.getName()) instanceof Date) {
                    tmp.append("'" + DateUtil.formatDateTime(Convert.toDate((ReflectUtil.getFieldValue(t, field.getName())))) + "',");
                }
                else {
                    tmp.append(ReflectUtil.getFieldValue(t, field.getName()) + ",");
                }
                sql += tmp;
            }
        }
        sql = StrUtil.removeSuffix(sql, ",") + " where " + id + "='" + ReflectUtil.getFieldValue(t, id)+"'";
        return sql;
    }

    /**
     * 生成删除语句
     * 根据 user 中第一个不为空的字段删除,应该尽量使用 id,提供至少一个非空属性
     * @param tablename 表明
     * @param t 有数据的实体
     * @param <T> 数据实体类型 如 User
     */
    public static  <T> String getDeleteSql(String tablename, T t) throws IllegalArgumentException {
        //delete from table_name where column_name = value
        return getSelectOrDeleteSql(tablename, t, "delete");
    }

    /**
     * 生成查询语句
     * 根据 user 中第一个不为空的字段查询
     * @param tablename 表名
     * @param t 有数据的实体
     * @param <T> 数据实体类型 如 User
     */
    public static  <T> String getSelectSql(String tablename, T t) throws IllegalArgumentException {
        //delete from table_name where column_name = value
        return getSelectOrDeleteSql(tablename, t, "select *");
    }

    /**
     * 根据 operation 生成一个如：operation from table_name where column_name = value 的sql语句
     * @param tablename
     * @param t
     * @param operation "select *"  or "delete"
     * @param <T>
     * @return
     * @throws IllegalArgumentException
     */
    private static  <T> String getSelectOrDeleteSql(String tablename, T t, String operation) throws IllegalArgumentException {
        //operation from table_name where column_name = value
        boolean flag = false;
        String sql = "";
        Field[] fields = ReflectUtil.getFieldsDirectly(t.getClass(), false);
        StringBuffer topHalf = new StringBuffer(operation + " from " + tablename + " where ");
        for (Field field : fields) {
            if ("ID".equals(field.getName()) || "id".equals(field.getName())) {
                if (ReflectUtil.getFieldValue(t, field.getName()) != null && Convert.toInt(ReflectUtil.getFieldValue(t, field.getName())) != 0) {
                    //id 不为空
                    topHalf.append(field.getName() + " = " + ReflectUtil.getFieldValue(t, field.getName()));
                    flag = true;
                    break;
                }
            }
            else {
                if (ReflectUtil.getFieldValue(t, field.getName()) != null && (String)ReflectUtil.getFieldValue(t, field.getName()) != "") {
                    topHalf.append(field.getName() + " = '" + ReflectUtil.getFieldValue(t, field.getName()) + "'");
                    flag = true;
                    break;
                }
            }
        }
        if (!flag) {
            throw new IllegalArgumentException(t.getClass() +  "NullException.\nThere is no attribute that is not empty.You must provide an object with at least one attribute.");
        }
        sql = topHalf.toString();
        return sql;
    }

    /**
     * 根据数据库生成字段 例如 private Object a;
     * @param dbname 数据库名
     * @param tablename 表名称
     * @return 成员变量拼接后的字符串
     * @throws SQLException
     */
    /*public static String getPirvateObjectXxx(String dbname,String tablename) throws SQLException {
        Connection conn = *//*获取你的数据库连接*//*;
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select COLUMN_NAME,COLUMN_TYPE from information_schema.COLUMNS where table_name = '"+tablename+"' and table_schema = '"+dbname+"'");
        StringBuffer sb=new StringBuffer();
        while (rs.next()) {
            sb.append("private Object "+rs.getObject(1)+";\n");
        }
        System.out.print(sb.toString());
        rs.close();
        stat.close();
        conn.close();
        return sb.toString();
    }*/



}
