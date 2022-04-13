package com.ruozedata.hadoop.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author 阿左
 * @theme
 * @create 2021-04-30
 **/
public class IpUtilOfFile {

    public static Random random = new Random();
    public static Map<String, String> map = new HashMap<>();
    public static List<String> list = IpUtilOfFile.getIpInfo();


//    public static void main(String[] args) throws Exception {
//        IpUtilOfFile ipUtilOfFile = new IpUtilOfFile();
//        List list = new ArrayList<>();
//        list = IpUtilOfFile.getIpInfo();
//        IpUtilOfFile.test(list);
//    }

    public static Map<String, String> getIpDetails(){

        int size = list.size();
        int i = 0;
        while (i < size ) {
            //拿到一条ipInfo的四个字段信息信息
            String ipInfo = list.get(i);
            String[] splits = ipInfo.split(",");
            //获取ipAddr

            String s = splits[0];
            String[] sp = s.split("\\.");

            String ipAddr_1 = sp[0];
            String ipAddr_2 = sp[1];
            String ipAddr_3 = sp[2];
            //将ip信息的前三段作为key，ipinfo作为value 封装进map。
            String ipAddr = ipAddr_1 +"."+ipAddr_2 + "."+ipAddr_3;

            map.put(ipAddr,ipInfo);
          //  System.out.println(ipAddr+","+ipInfo + i);
            i++;
        }

        System.out.println("信息全部加载进map");

        return map;
    }


    public static List getIpInfo(){
        List<String> list = new ArrayList<>();
        BufferedReader bufferedReader;

        String ip = "";
        String province ="";
        String city = "";
        String isp = "";

        try {
            bufferedReader = new BufferedReader(new FileReader("data\\ip.txt"));
            String tempStr;
            while ((tempStr = bufferedReader.readLine()) != null) {
                String[] splits = tempStr.split("\\|");
                //223.85.28.0得到的ip全是以0结尾，为了接近真实，在最后一位加上255以内的随机数。
                String ip_1 = splits[0].trim().split("\\.")[0];
                String ip_2 = splits[0].trim().split("\\.")[1];
                String ip_3 = splits[0].trim().split("\\.")[2];
                ip = ip_1 +"."+ ip_2 +"."+ ip_3 +"."+random.nextInt(255);
                province = splits[6].trim();
                city = splits[7].trim();
                isp =splits[9].trim();
                String string = ip+","+province+","+city+","+isp;
                //1.0.1.172,福建,福州,电信
                list.add(string);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }
}
