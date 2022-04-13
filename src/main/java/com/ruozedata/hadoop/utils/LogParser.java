package com.ruozedata.hadoop.utils;

import com.ruozedata.hadoop.domain.Access;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.Random;

/**
 * @author 阿左
 * @theme 日志解析测试
 * @create 2021-04-29
 **/
public class LogParser {

    //加载一次，解析ip用到的数据库
    private static Map<String, String> ipMap = IpUtilOfFile.getIpDetails();
    private static int count = 0;
    private static int right = 0;
    private static int error = 0;

    public static void main(String[] args) throws Exception  {

        //String log = "[11/06/2021:20:51:07 +0800]\t182.90.24.6\t-\t111\t-\tPOST\thttp://www.ruozedata.com/video/av90874?a=b&c=d\t200\t38\t821\tHIT";


        BufferedReader bufferedReader = new BufferedReader(new FileReader("data/access20210504.txt"));
        String log;

        while ((log = bufferedReader.readLine()) != null) {
            Boolean flag = true;
            String[] splits = log.split("\t");

            //TODO...日志转为assecc
            String time = splits[0];
            String ip = splits[1];
            String proxyIp = splits[2];
            String responseTime = splits[3];
            String referer = splits[4];
            String method = splits[5];
            String url = splits[6];
            String httpCode = splits[7];
            String requestSize = splits[8];
            String responseSize = splits[9];
            String cache = splits[10];

            Access access = new Access();
            access.setIp(ip);
            access.setProxyIp(proxyIp);
            access.setReferer(referer);
            access.setMethod(method);

            access.setRequestSize(Long.parseLong(requestSize));

            access.setHttpCode(httpCode);
            access.setCache(cache);
            try {
            access.setResponseTime(Long.parseLong(responseSize));
                right++;
            }catch (NumberFormatException e) {
                error++;
            }finally {
                count++;
            }
            access.setResponseTime(Long.parseLong(responseTime));

            //解析url
            URL u = new URL(url);
            String domain = u.getHost();
            String path = u.getPath();
            String protocol = u.getProtocol();
            String params = u.getQuery();
            access.setDomain(domain);
            if (path.length() != 0){
                access.setPath(path);
            }else {
                access.setPath("-");
            }
            access.setHttp(protocol);
            if (params != null){
                access.setParams(params);
            }else{
                access.setParams("-");
            }

            //使用ip文件库读取解析ip

            String ipAddr_1 = ip.split("\\.")[0];
            String ipAddr_2 = ip.split("\\.")[1];
            String ipAddr_3 = ip.split("\\.")[2];
            String ipAddr = ipAddr_1+"."+ipAddr_2 + "."+ipAddr_3;

            String ipInfo = ipMap.get(ipAddr);
            String[] ipInfos = ipInfo.split(",");
            String province = ipInfos[1];
            String city = ipInfos[2];
            String isp = ipInfos[3];

            access.setProvince(province.length() == 0 ? "-" : province);
            access.setCity(city.length() == 0 ? "-" : city);
            access.setIsp(isp.trim().length() == 0 ? "-" : isp);

            //解析time
            SimpleDateFormat format = new SimpleDateFormat("[dd/MM/yyyy:HH:mm:ss +0800]");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(format.parse(time));
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;//月份从0开始，需要+1
            int day = calendar.get(Calendar.DATE);
            access.setYear(year + "");
            //月份的坑，1月 12月  => 月份补齐
            access.setMonth(month < 10 ? "0" + month : month + "");
            access.setDay(day < 10 ? "0" + day : day + "");


            System.out.println(access.toString());
        }

        System.out.println("error:"+error+"\t"+"right:"+right+"\t"+"count:"+count);
    }
}
