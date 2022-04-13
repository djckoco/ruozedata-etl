package com.ruozedata.hadoop.mapreduce;

import com.ruozedata.hadoop.domain.Access;
import com.ruozedata.hadoop.utils.FileUtils;
import com.ruozedata.hadoop.utils.IpUtilOfFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

/**
 * @author 阿左
 * @theme
 * @create 2021-05-02
 **/
public class ETLDriver {
    public static void main(String[] args) throws Exception {
        String input = "data/access20210521.txt";
        String output = "out";
//        String input = args[0];//"data/access20210502.txt";
//        String output = args[1];//"out";

        Configuration conf = new Configuration();
        //0，使用工具类确保输出路径不存在，删除目标文件
        FileUtils.deleteTarget(conf, output);

        //1，获取job对象
        Job job = Job.getInstance(conf);

        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(MyMapper.class);

        //4，设置Mapper阶段输出的k v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //ETL 不需要reduce
        job.setNumReduceTasks(0);

        //6，设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        //7，提交
        boolean b = job.waitForCompletion(true);

        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        //得到日志清洗情况
        CounterGroup counterGroup = job.getCounters().getGroup("etl");
        Iterator<Counter> iterator = counterGroup.iterator();
        while (iterator.hasNext()) {
            Counter counter = iterator.next();
            System.out.println(counter.getName() + "--->" + counter.getValue());
        }

        // 能拿到日志清洗情况： 把这些数据指标写入到某个地方就可以了
        /**
         * 元数据管理：批次、总记录数、错误记录数、正确记录数、耗费时间、开始时间、结束时间....
         */

        System.exit(b ? 0 : 1);
    }

    /**
     * ETL 清洗过后输出的KV类型？
     * key ：text
     * value ：nullWritable
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private static Map<String, String> ipMap = IpUtilOfFile.getIpDetails();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //System.out.println("---------------------");
            try {
                //计数器，这个批次对应的总的记录数，符合规范的记录数，不符合规范的记录数
                context.getCounter("etl","totals").increment(1);
                String[] splits = value.toString().split("\t");

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

                /**
                 * h核心字段处理：核心字段出错，那么整条数据都应该丢弃
                 * 外层有tay cache，如果程序出错，捕捉进入cache，那么本条数据将丢弃，不记录
                 */
                access.setRequestSize(Long.parseLong(requestSize));

                access.setHttpCode(httpCode);
                access.setCache(cache);
                /**
                 * 非核心字段，特殊处理（类型不匹配）方法
                 * responseSize,非核心字段，可以允许出错，例如出现为非long类型的值。
                 * 此时如果将本条数据当成脏数据处理，势必会引起核心指数的计算出错。
                 * 故，非核心字段处理时，当出现错误字段时，使用默认参数。
                 */
                long resSize = 0;
                try {
                    resSize = Long.parseLong(responseSize);//非核心字段，特殊处理
                }catch (Exception e){//字段不匹配，则不作处理，直接使用默认参数0
                //捕捉，不处理
                }
                access.setResponseSize(resSize);

                long resTime = 0;
                try {
                    resTime = Long.parseLong(responseTime);//非核心字段，特殊处理
                }catch (Exception e){
                //捕捉，不处理
                }
                access.setResponseTime(resTime);

                //核心字段，若果出错，不记录本条数据
                access.setRequestSize(Long.parseLong(requestSize));

                //解析url
                URL u = new URL(url);
                String domain = u.getHost();
                String path = u.getPath();
                String protocol = u.getProtocol();
                String params = u.getQuery();
                access.setDomain(domain);
                if (path.length() != 0) {
                    access.setPath(path);
                } else {
                    access.setPath("-");
                }
                access.setHttp(protocol);
                if (params != null) {
                    access.setParams(params);
                } else {
                    access.setParams("-");
                }


                //使用ip文件库读取解析ip

                String ipAddr_1 = ip.split("\\.")[0];
                String ipAddr_2 = ip.split("\\.")[1];
                String ipAddr_3 = ip.split("\\.")[2];
                String ipAddr = ipAddr_1 + "." + ipAddr_2 + "." + ipAddr_3;

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
            try {
                calendar.setTime(format.parse(time));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONTH) + 1;//月份从0开始，需要+1
                int day = calendar.get(Calendar.DATE);
                access.setYear(year + "");
                //月份的坑，1月 12月  => 月份补齐
                access.setMonth(month < 10 ? "0" + month : month + "");
                access.setDay(day < 10 ? "0" + day : day + "");

                //符合规范的记录数
                context.getCounter("etl","formats").increment(1);
                //执行到这里代表数据解析ok，可以提交
                context.write(new Text(access.toString()), NullWritable.get());
            }catch (Exception e) {
                //不符合规范的记录数
                context.getCounter("etl", "error").increment(1);
            }
        }
    }
}
