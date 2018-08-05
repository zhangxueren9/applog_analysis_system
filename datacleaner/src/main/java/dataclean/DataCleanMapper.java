package dataclean;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text outKey = null;
    NullWritable outValue = null;
    SimpleDateFormat sdf = null;
    MultipleOutputs<Text,NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outValue=NullWritable.get();
        sdf = new SimpleDateFormat("yyyy-mm-dd hh:MM:ss");
        mos = new MultipleOutputs<Text,NullWritable>(context);
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject jsonObj = JSONObject.parseObject(value.toString());
        JSONObject header = jsonObj.getJSONObject(GlobalConstants.header);

        //若缺失以下信息则将该行数据舍弃
        if (StringUtils.isBlank(header.getString("sdk_ver"))) {
            return;
        }

        if (null == header.getString("time_zone") || "".equals(header.getString("time_zone").trim())) {
            return;
        }

        if (null == header.getString("commit_id") || "".equals(header.getString("commit_id").trim())) {
            return;
        }

        if (null == header.getString("commit_time") || "".equals(header.getString("commit_time").trim())) {
            return;
        }else{
            // 练习时追加的逻辑，替换掉原始数据中的时间戳
            String commit_time = header.getString("commit_time");
            String format = sdf.format(new Date(Long.parseLong(commit_time)+38*24*60*60*1000L));
            header.put("commit_time", format);

        }

        if (null == header.getString("pid") || "".equals(header.getString("pid").trim())) {
            return;
        }

        if (null == header.getString("app_token") || "".equals(header.getString("app_token").trim())) {
            return;
        }

        if (null == header.getString("app_id") || "".equals(header.getString("app_id").trim())) {
            return;
        }

        if (null == header.getString("device_id") || header.getString("device_id").length()<17) {
            return;
        }

        if (null == header.getString("device_id_type")
                || "".equals(header.getString("device_id_type").trim())) {
            return;
        }

        if (null == header.getString("release_channel")
                || "".equals(header.getString("release_channel").trim())) {
            return;
        }

        if (null == header.getString("app_ver_name") || "".equals(header.getString("app_ver_name").trim())) {
            return;
        }

        if (null == header.getString("app_ver_code") || "".equals(header.getString("app_ver_code").trim())) {
            return;
        }

        if (null == header.getString("os_name") || "".equals(header.getString("os_name").trim())) {
            return;
        }

        if (null == header.getString("os_ver") || "".equals(header.getString("os_ver").trim())) {
            return;
        }

        if (null == header.getString("language") || "".equals(header.getString("language").trim())) {
            return;
        }

        if (null == header.getString("country") || "".equals(header.getString("country").trim())) {
            return;
        }

        if (null == header.getString("manufacture") || "".equals(header.getString("manufacture").trim())) {
            return;
        }

        if (null == header.getString("device_model") || "".equals(header.getString("device_model").trim())) {
            return;
        }

        if (null == header.getString("resolution") || "".equals(header.getString("resolution").trim())) {
            return;
        }

        if (null == header.getString("net_type") || "".equals(header.getString("net_type").trim())) {
            return;
        }

        String userId = header.getString("device_id");
        if(header.getString("os_name").contains("android") && null != header.getString("android_id")){
            userId = header.getString("android_id");
        }
        
        header.put("userId",userId);
        outKey = new Text(header.toJSONString());
       if("android".equals(header.getString("os_name"))){
           mos.write(outKey,outValue,"android/android");
       }else if("ios".equals(header.getString("os_name"))){
           mos.write(outKey,outValue,"ios/ios");
       }else {
           mos.write(outKey,outValue,"others/others");
       }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
       mos.close();
    }
}
