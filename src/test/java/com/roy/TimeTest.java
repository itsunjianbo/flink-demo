package com.roy;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author roy
 * @date 2021/9/14
 * @desc
 */
public class TimeTest {
    public static void main(String[] args) throws ParseException {
//        String date = "2021-09-14 11:42:31";
//        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
//        final Date parsedDate = sdf.parse(date);
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(parsedDate);
//        System.out.println(cal.getTimeInMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//        System.out.println(Time.minutes(10).toMilliseconds());
//        Date date = new Date(1631689800000L);
//        System.out.println(sdf.format(date));
//        System.out.println(sdf.format(new Date(1631690405000L)));
//        date = new Date(1631690400000L);
//        System.out.println(sdf.format(date));
//        date = new Date(1631691000000L);
//        System.out.println(sdf.format(date));

//        System.out.println(sdf.format(new Date(1631694000000L)));
//        System.out.println(sdf.format(new Date(1631695081000L)));
//        System.out.println(sdf.format(new Date(1631694600000L)));
//        System.out.println(sdf.format(new Date(1631692025000L)));
//        System.out.println(sdf.format(new Date(1631691600000L)));
//        System.out.println(sdf.format(new Date(1631692625000L)));
//        System.out.println(sdf.format(new Date(1631692200000L)));
//        System.out.println(sdf.format(new Date(1631693224000L)));
//        System.out.println(sdf.format(new Date(1631692800000L)));
//        System.out.println(sdf.format(new Date(1631693821000L)));
//        System.out.println(sdf.format(new Date(1631693400000L)));

        System.out.println(sdf.parse("2021-09-15 00:00:00").getTime());
        System.out.println(sdf.parse("2021-09-15 00:01:11").getTime());
        System.out.println(sdf.parse("2021-09-15 00:01:12").getTime());
        System.out.println(sdf.parse("2021-09-15 00:01:14").getTime());
        System.out.println(sdf.parse("2021-09-15 00:01:15").getTime());
        System.out.println(sdf.parse("2021-09-15 00:06:20").getTime());
        System.out.println(sdf.parse("2021-09-15 00:12:31").getTime());
        System.out.println(sdf.parse("2021-09-16 00:00:00").getTime());
    }
}
