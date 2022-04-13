package com.ruozedata.hadoop.domain;

/**
 * 日志数据字段
 *
 * access => ETL => load Hive table
 *
 * url & referer 大小占整个日志的70%。
 **/
public class Access {

    //[11/06/2021:20:51:07 +0800]	182.90.24.6	-	111	-	POST	http://www.ruozedata.com	200	38	821	HIT

    private String time;    //hive表中可以不用保存（分区表） y,m,d
    private String ip;      //province city isp(电信运营商)
    private String proxyIp;
    private long responseTime;  //Long.parselong特殊处理
    private String referer;
    private String method;
    //private String url;     //http https head、domain、path、params
    private String httpCode;
    private long requestSize;   //Long.parselong特殊处理
    private long responseSize;  //Long.parselong特殊处理
    private String cache;

    //time扩展字段
    private String year;
    private String month;
    private String day;

    //ip扩展字段
    private String province;
    private String city;
    private String isp;

    //url扩展字段
    private String http;
    private String domain;
    private String path;
    private String params;

    @Override
    public String toString() {
        return  ip + "\t" +
                proxyIp + "\t" +
                responseTime + "\t" +
                referer + "\t" +
                method + "\t" +
                httpCode + "\t" +
                requestSize + "\t" +
                responseSize + "\t" +
                cache + "\t" +
                province + "\t" +
                city + "\t" +
                isp + "\t" +
                http + "\t" +
                domain + "\t" +
                path + "\t" +
                params + "\t" +
                year + "\t" +
                month + "\t" +
                day;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getProxyIp() {
        return proxyIp;
    }

    public void setProxyIp(String proxyIp) {
        this.proxyIp = proxyIp;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(String httpCode) {
        this.httpCode = httpCode;
    }

    public long getRequestSize() {
        return requestSize;
    }

    public void setRequestSize(long requestSize) {
        this.requestSize = requestSize;
    }

    public long getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(long responseSize) {
        this.responseSize = responseSize;
    }

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIsp() {
        return isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getHttp() {
        return http;
    }

    public void setHttp(String http) {
        this.http = http;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }
}
