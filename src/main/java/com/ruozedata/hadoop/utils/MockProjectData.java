package com.ruozedata.hadoop.utils;

import java.io.*;
import java.util.List;
import java.util.Random;


/**
 * 产生日志的工具类
 */
public class MockProjectData {

    public static IpUtilOfFile ipUtilOfFile;
    List<String> ipInfoList = ipUtilOfFile.getIpInfo();
    Random random = new Random();

    public static void main(String[] args){
        MockProjectData projectData = new MockProjectData();
        //造num条数据
        projectData.mockAllData(600000,"20/07/2021");
        //projectData.mockUrl();
    }

    //[11/06/2021:20:51:07 +0800]	182.90.24.6	-	111	-	POST	http://www.ruozedata.com	200	38	821	HIT
    //倒数第二个字段：造10%的脏数据【字符串类型】
    void mockAllData(int num,String currentDate){

        //需要的11个字段。
        String time;
        String ip;
        String proxyIp;
        long responseTime;
        String referer;
        String method;
        String url;
        String httpCode;
        long requestSize;
        String responseSize;
        String cache;


        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("data/20210720"))));

            int i = 0;
            while (i++ < num) {
                //产生日志字段信息
                //[11/06/2021:20:51:07 +0800]
                int s = random.nextInt(60);
                int M = random.nextInt(60);
                int h = random.nextInt(24);

                String ss = s < 10 ? "0"+s : s+"";
                String MM = M < 10 ? "0"+M : M+"";
                String hh = h < 10 ? "0"+h : h+"";


                time = "[" + currentDate +":"+hh+":"+MM+":" +ss+ " +0800]";

                ip = mockIp();

                //proxyIp  5%的日志有proxyIp字段
                if(random.nextInt(20) == 5){
                    proxyIp = mockIp();
                }else {
                    proxyIp = "-";
                }

                responseTime = random.nextInt(2500);

                referer = random.nextInt(500) + "";

                //多产生post，get
                String[] methods = {"POST","GET","POST","GET","HEAD"};
                method = methods[random.nextInt(2)];

                url = mockUrl();
                //多产生200

                String[] httpCodes = {"200","200","200","200","301","404","500"};
                httpCode = httpCodes[random.nextInt(6)];

                //请求大小，字节
                requestSize = random.nextInt(500);

                //请求返回大小，字节，此字段制造10%脏数据
                if(random.nextInt(10) == 5){//脏数据
                    responseSize = "-";
                }else {//正确数据
                    responseSize = random.nextInt(1000) +"";
                }


                String[] caches = {"HIT","HIT","MISS"};
                cache = caches[random.nextInt(3)];

                String result = time + "\t" +
                        ip + "\t" +
                        proxyIp + "\t" +
                        responseTime+ "\t" +
                        referer + "\t" +
                        method + "\t" +
                        url + "\t" +
                        httpCode + "\t" +
                        requestSize + "\t" +
                        responseSize + "\t" +
                        cache+"\n";
                writer.write(result);
            }
            writer.flush();
            writer.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //制造ip字段
    String mockIp()  {

        //随机获取一条数据
        String ipInfo = ipInfoList.get(random.nextInt(ipInfoList.size()));

        //返回ipAddr
        //String str = ipInfo.split()
        //return ipInfo.split(",")[0];
        return ipInfo.trim();
    }

    String mockUrl(){

        //随机返回一条数据
        String url;

        String[] urls = {"https://www.bilibili.com","http://www.ruozedata.com","https://ruoze.ke.qq.com"};
        int i = random.nextInt(urls.length);
        if (i == 0) {
            url = urls[0] +"/video"+"/av"+random.nextInt(99999999);
            if(random.nextInt(10) == 1){
                url = url +"?a=b&c=d";
            }
        }else{
            url = urls[i];
        }

        //System.out.println(url);
        //返回一条url
        return url;
    }

}
