package com.cablevision.yyang.hive.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class UDFSubMicrosec extends UDF{
    public Text evaluate(Text src_ts,IntWritable microSec) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String format = "^(((20[0-3][0-9]-(0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|(20[0-3][0-9]-(0[2469]|11)-(0[1-9]|[12][0-9]|30))) (20|21|22|23|[0-1][0-9]):[0-5][0-9]:[0-5][0-9])\\.\\d{3}$";
        Pattern p = Pattern.compile(format);
        Matcher m = p.matcher(src_ts.toString());
        if (m.matches()) {
            Date parsedDate = dateFormat.parse(src_ts.toString());
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(parsedDate);
            calendar.set(Calendar.MILLISECOND, calendar.get(Calendar.MILLISECOND) - microSec.get());
            
            String res = dateFormat.format(calendar.getTime());
            return new Text(res.toString());
        } else {
            return new Text("Make sure your date format is yyyy-MM-dd HH:mm:ss.SSS");
        }
    }
}