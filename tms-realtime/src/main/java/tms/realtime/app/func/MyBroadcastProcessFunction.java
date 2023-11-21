package tms.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import tms.realtime.beans.TmsConfigDimBean;
import tms.realtime.utils.DateFormatUtil;

import java.sql.*;
import java.util.*;

/**
 * @author xiaojia
 * @date 2023/11/2 10:15
 * @desc 自定义类完成对主流和广播流数据的处理
 */
public class MyBroadcastProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor;
    private Map<String,TmsConfigDimBean> configMap=new HashMap<>();
    private String user;
    private String password;

    public MyBroadcastProcessFunction(MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor,String[] args) {
        this.mapStateDescriptor = mapStateDescriptor;
        ParameterTool parameterTool=ParameterTool.fromArgs(args);
        this.user = parameterTool.get("mysql-username", "root");
        this.password = parameterTool.get("mysql-password", "123456");
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的数据进行预加载
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop101:3306/tms_config?useSSL=false&useUnicode=true" +
                "&user=" + user + "&password=" + password +
                "&charset=utf8&TimeZone=Asia/Shanghai");
        PreparedStatement ps = conn.prepareStatement("select * from tms_config.tms_config_dim");
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while(rs.next()){
            JSONObject jsonObject = new JSONObject();
            for(int i=1;i<=metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObject.put(columnName,columnValue);
            }
            TmsConfigDimBean tmsConfigDimBean = jsonObject.toJavaObject(TmsConfigDimBean.class);
            configMap.put(tmsConfigDimBean.getSourceTable(),tmsConfigDimBean);
        }
        rs.close();
        ps.close();
        conn.close();
    }

    //处理主流业务数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //获取主流数据操作的表名
        String table = jsonObject.getString("table");
        ReadOnlyBroadcastState<String, TmsConfigDimBean> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TmsConfigDimBean tmsConfigDimBean = null;
        if((tmsConfigDimBean=broadcastState.get(table))!=null||(tmsConfigDimBean=configMap.get(table))!=null){
            //如果对应的配置信息不为空 说明是维度数据
            JSONObject after = jsonObject.getJSONObject("after");
                // 数据脱敏
                switch (table) {
                    // 员工表信息脱敏
                    case "employee_info":
                        String empPassword = after.getString("password");
                        String empRealName = after.getString("real_name");
                        String idCard = after.getString("id_card");
                        String phone = after.getString("phone");

                        // 脱敏
                        empPassword = DigestUtils.md5Hex(empPassword);
                        empRealName = empRealName.charAt(0) +
                                empRealName.substring(1).replaceAll(".", "\\*");
                        //知道有这个操作  idCard是随机生成的，和标准的格式不一样 所以这里注释掉
                        // idCard = idCard.matches("(^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$)|(^[1-9]\\d{5}\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{2}$)")
                        //     ? DigestUtils.md5Hex(idCard) : null;
                        phone = phone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                                ? DigestUtils.md5Hex(phone) : null;

                        after.put("password", empPassword);
                        after.put("real_name", empRealName);
                        after.put("id_card", idCard);
                        after.put("phone", phone);
                        break;
                    // 快递员信息脱敏
                    case "express_courier":
                        String workingPhone = after.getString("working_phone");
                        workingPhone = workingPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                                ? DigestUtils.md5Hex(workingPhone) : null;
                        after.put("working_phone", workingPhone);
                        break;
                    // 卡车司机信息脱敏
                    case "truck_driver":
                        String licenseNo = after.getString("license_no");
                        licenseNo = DigestUtils.md5Hex(licenseNo);
                        after.put("license_no", licenseNo);
                        break;
                    // 卡车信息脱敏
                    case "truck_info":
                        String truckNo = after.getString("truck_no");
                        String deviceGpsId = after.getString("device_gps_id");
                        String engineNo = after.getString("engine_no");

                        truckNo = DigestUtils.md5Hex(truckNo);
                        deviceGpsId = DigestUtils.md5Hex(deviceGpsId);
                        engineNo = DigestUtils.md5Hex(engineNo);

                        after.put("truck_no", truckNo);
                        after.put("device_gps_id", deviceGpsId);
                        after.put("engine_no", engineNo);
                        break;
                    // 卡车型号信息脱敏
                    case "truck_model":
                        String modelNo = after.getString("model_no");
                        modelNo = DigestUtils.md5Hex(modelNo);
                        after.put("model_no", modelNo);
                        break;
                    // 用户地址信息脱敏
                    case "user_address":
                        String addressPhone = after.getString("phone");
                        addressPhone = addressPhone.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                                ? DigestUtils.md5Hex(addressPhone) : null;
                        after.put("phone", addressPhone);
                        break;
                    // 用户信息脱敏
                    case "user_info":
                        String passwd = after.getString("passwd");
                        String realName = after.getString("real_name");
                        String phoneNum = after.getString("phone_num");
                        String email = after.getString("email");

                        // 脱敏
                        passwd = DigestUtils.md5Hex(passwd);
                        if(StringUtils.isNotEmpty(realName)){
                            realName = DigestUtils.md5Hex(realName);
                            after.put("real_name", realName);
                        }
                        phoneNum = phoneNum.matches("^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$")
                                ? DigestUtils.md5Hex(phoneNum) : null;
                        email = email.matches("^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$")
                                ? DigestUtils.md5Hex(email) : null;

                        after.put("birthday", DateFormatUtil.toDate(after.getInteger("birthday") * 24 * 60 * 60 * 1000L));
                        after.put("passwd", passwd);
                        after.put("phone_num", phoneNum);
                        after.put("email", email);
                        break;
                }

            //过滤掉不需要的属性
            String sinkColumns = tmsConfigDimBean.getSinkColumns();
            filterColumn(after,sinkColumns);
            String sinkTable = tmsConfigDimBean.getSinkTable();
            String sinkPk = tmsConfigDimBean.getSinkPk();
            after.put("sink_table",sinkTable);
            after.put("sink_pk",sinkPk);

            //清除Redis缓存的准备工作(传递操作类型----传递可以用于查询当前维度表的外键字段名以及对应的值)
            String op = jsonObject.getString("op");
            if("u".equals(op)){
                //将操作类型传递到下游
                after.put("op",op);

                //从配置表中获取当前维度表关联的外键名
                String foreignKeys = tmsConfigDimBean.getForeignKeys();
                //定义一个json对象，用于存储当前维度表对应的外键名以及外键值
                JSONObject foreignJsonObj = new JSONObject();
                if(StringUtils.isNotEmpty(foreignKeys)){
                    String[] foreignNameArr = foreignKeys.split(",");
                    for (String foreignName : foreignNameArr) {
                        //获取修改前的数据
                        JSONObject before = jsonObject.getJSONObject("before");
                        //获取外键修改前的值
                        String foreignKeyBefore = before.getString(foreignName);
                        //获取外键修改后的值
                        String foreignKeyAfter = after.getString(foreignName);
                        if(!foreignKeyBefore.equals(foreignKeyAfter)){
                            //如果修改的是外键
                            foreignJsonObj.put(foreignName,foreignKeyBefore);
                        }else{
                            foreignJsonObj.put(foreignName,foreignKeyAfter);
                        }
                    }
                }
                after.put("foreign_key",foreignJsonObj);
            }

            //将维度数据传递到下游
            collector.collect(after);
        }
    }

    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(entry->!fieldList.contains(entry.getKey()));
    }

    //处理广播流业务数据
    @Override
    public void processBroadcastElement(String jsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject jsonObject = JSON.parseObject(jsonStr);
        //获取广播状态
        BroadcastState<String, TmsConfigDimBean> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //获取对配置表的操作类型
        String op = jsonObject.getString("op");
        if ("d".equals(op)) {
            String sourceTable = jsonObject.getJSONObject("before").getString("source_table");
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            TmsConfigDimBean configDimBean = jsonObject.getObject("after", TmsConfigDimBean.class);
            String sourceTable = configDimBean.getSourceTable();
            broadcastState.put(sourceTable,configDimBean);
            configMap.put(sourceTable,configDimBean);
        }
    }
}
