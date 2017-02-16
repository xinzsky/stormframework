# 简介
stormframework是对storm开发的一个封装，目的是为了让开发人员只需专注于数据处理或分析逻辑的开发即可，不需要了解Kafka、Storm、HDFS方面的知识。  
stormframework会自动从kafka读取消息，回调消息处理方法，并会把消息保存到HDFS。
## 特性
* 采用storm-kafka来读取kafka消息
* 采用storm-hdfs来保存消息到HDFS
* 支持HDFS Namenode高可用
* 支持消息的顺序处理

## 开发接口说明
* 接口文件：src/main/java/bigdata/Streaming.java
* 接口说明：
    * 框架会回调Streaming类中的方法：open() process() close()。
    * public void open(String args);
      * 此方法在处理消息之前调用，且只调用一次，一般用于消息处理初始化工作，比如读取配置文件、连接数据库等。
      * 参数@args是由消息处理程序运行时通过命令行参数指定的，框架回调open()方法时会把此参数传递给open()方法。
    * public String process(String key, String message);
      * 此方法用于对消息的处理，每一条消息都会调用此方法进行处理。
      * 参数@key：消息的key。
      * 参数@message: 实际要处理的消息，数据格式自行定义。 
      * 返回值：错误返回null，需要过滤掉的消息则返回空串""，处理ok则返回message。
    * public void close();
      * 此方法会在消息处理程序退出时调用，且只调用一次，一般用于消息处理的清理工作，比如关闭文件、数据库连接等。
