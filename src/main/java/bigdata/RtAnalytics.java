package bigdata;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.StringKeyValueScheme;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.ZkHosts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.commons.cli.CommandLineParser;  
import org.apache.commons.cli.BasicParser;  
import org.apache.commons.cli.Options;  
import org.apache.commons.cli.CommandLine;  
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import bigdata.Streaming;
import bigdata.MyStringScheme;
import bigdata.MyRecordFormat;
import bigdata.MyHdfsBolt;

public class RtAnalytics 
{
    private String topic;
    private SpoutConfig spoutConf;
    private KafkaSpout kafkaSpout;
    private MyHdfsBolt hdfsBolt;

    private static final String MSG_KEY = "key";
    private static final String MSG_VALUE = "value";

    public RtAnalytics(String topic) 
    { 
        this.topic = topic;
    }

    private void kafkaSpout(String groupId, String topic, String zkServers, String offsetZkRoot, boolean fromStart) 
    {
        BrokerHosts brokerHosts = new ZkHosts(zkServers);
        // brokerHosts.refreshFreqSecs = 60;
        spoutConf = new SpoutConfig(brokerHosts, topic, offsetZkRoot, groupId);
        spoutConf.scheme = new SchemeAsMultiScheme(new MyStringScheme());
        //spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
        spoutConf.forceFromStart = fromStart;
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        //spoutConf.ignoreZkOffsets = false;

        // for LocalCluster test
        //spoutConf.zkServers = Arrays.asList(new String[] {"test1", "test2", "test3"});
        //spoutConf.zkPort = 2181;

        kafkaSpout = new KafkaSpout(spoutConf);
    }

    public static class StreamBolt implements IRichBolt   
    {
        private OutputCollector collector;
        private String args;
        private Streaming streaming;
        private static final Log LOG = LogFactory.getLog(StreamBolt.class);

        public StreamBolt(String args) {
           this.args = args;

        }
 
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
           this.collector = collector;      
           LOG.info("[Streaming] open, args:  " + args);
           streaming = new Streaming();  
           streaming.open(args);
        }

        @Override
        public void execute(Tuple tuple) {
           String key = tuple.getStringByField(MSG_KEY);
           String value = tuple.getStringByField(MSG_VALUE);
           String msg = streaming.process(key, value);
           if(msg == null) {
             collector.fail(tuple);
           } else if(msg.equals("")) {
             collector.ack(tuple);
           } else {
             collector.emit(tuple, new Values(key, msg));
             collector.ack(tuple);
           }
        }

        @Override
        public void cleanup() {
          LOG.info("[Streaming] close ...  ");

          streaming.close();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
             declarer.declare(new Fields(MSG_KEY, MSG_VALUE));    
        }

        @Override
        public Map getComponentConfiguration() {
            return null;
        }
    }

    private void hdfsBolt(String namenode, String path, String nnhaConfig) 
    {
      RecordFormat format = new MyRecordFormat().withFieldDelimiter("\t").excludeField(MSG_KEY);
      FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(path).withPrefix(this.topic + "-").withExtension(".txt"); 
      SyncPolicy syncPolicy = new CountSyncPolicy(1000); 
      FileRotationPolicy rotationPolicy = new TimedRotationPolicy(10.0f, TimeUnit.DAYS);
      
      hdfsBolt = (MyHdfsBolt)(new MyHdfsBolt(nnhaConfig)
                .withFsUrl(namenode)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy));
    }

    private void runTopology(String topologyName, int tasknum, int workernum, boolean localmode, String streamargs) throws 
                 AlreadyAliveException, InvalidTopologyException, InterruptedException
    {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", kafkaSpout, tasknum);
        builder.setBolt("streaming", new StreamBolt(streamargs), tasknum).fieldsGrouping("kafka-reader", new Fields(MSG_KEY));
        builder.setBolt("hdfs-writer", hdfsBolt, tasknum).fieldsGrouping("streaming", new Fields(MSG_KEY));
    
        // submit topology
        Config conf = new Config();
        String name = topologyName;
        if (!localmode) {
           conf.setNumWorkers(workernum);
           conf.setMaxSpoutPending(1000);   // 增加pending tuple数，默认为1，设置太大造成ack时间超时而fail
           conf.setMessageTimeoutSecs(90);  // 增加等待ack超时时间，默认为30s
           conf.setNumAckers(0);            // 增加acker线程数，默认为1

           StormSubmitter.submitTopology(name, conf, builder.createTopology());
        } else {
           LocalCluster cluster = new LocalCluster();
           conf.setMaxTaskParallelism(3);
           conf.setDebug(true);
           conf.setNumWorkers(3);
           cluster.submitTopology(name, conf, builder.createTopology());
           Thread.sleep(50000);
           cluster.killTopology(name);
           cluster.shutdown();
        }
    }

    public static void main(String[] args)
    { 
        Options options = new Options();  
        options.addOption("n", "namenode",  true,  "hdfs://host:port or hdfs://nameservice");  
        options.addOption("H", "HA",        true,  "namenode HA config path");
        options.addOption("p", "path",      true,  "hdfs store path");  
        options.addOption("z", "zkServers", true,  "zookeeper servers, format: host1:port1,host2:port2[/zkroot]");
        options.addOption("o", "offset",    true,  "offset zookeeper root, default: /{TopicName}Offsets");
        options.addOption("t", "topic",     true,  "data topic");
        options.addOption("g", "groupId",   true,  "consumer group");
        options.addOption("S", "start",     false, "force from start");  
        options.addOption("N", "name",      true,  "topology name");  
        options.addOption("w", "workernum", true,  "worker number, default: 3");  
        options.addOption("T", "tasknum",   true,  "task number = topic partitions, default: 3");  
        options.addOption("s", "args",      true,  "streaming arguments"); 
        options.addOption("l", "localmode", false, "local mode is useful for developing and testing topologies. " );  
        options.addOption("h", "help",      false, "print this usage information");  
       
        CommandLineParser parser;
        CommandLine commandLine; 
        HelpFormatter hf = new HelpFormatter();
        String namenode, path, zkServers, topic, offsetZkRoot, nnhaConfig;
        
        try {
            parser = new BasicParser( ); 
            commandLine = parser.parse(options, args); 
        } catch(ParseException e) {
            hf.printHelp("RtAnalytics", options, true);
            return;
        } 

        if(commandLine.hasOption('h') ) {  
            hf.printHelp("RtAnalytics", options, true);
            return;
        } 
        
        if(commandLine.hasOption('n') ) {  
            namenode = commandLine.getOptionValue('n');  
        } else {
            System.out.println("namenode isn't specified");
            return;
        }

        nnhaConfig = "";
        if(commandLine.hasOption('H') ) {
            nnhaConfig = commandLine.getOptionValue('H');
        }

        if(commandLine.hasOption('p') ) {  
            path = commandLine.getOptionValue('p');  
        } else {
            System.out.println("hdfs store path isn't specified");
            return;
        }

        if(commandLine.hasOption('z') ) {  
            zkServers = commandLine.getOptionValue('z');  
        } else {
            System.out.println("zookeeper servers isn't specified");
            return;
        }
        
        if(commandLine.hasOption('t') ) {  
            topic = commandLine.getOptionValue('t');  
        } else {
            System.out.println("topic isn't specified");
            return;
        }
        
        offsetZkRoot = "/" + topic + "Offsets";
        if(commandLine.hasOption('o') ) {  
            offsetZkRoot = commandLine.getOptionValue('o');  
        }

        String topologyName = "RtAnalytics";
        if(commandLine.hasOption('N') ) {
            topologyName = commandLine.getOptionValue('N');
        }

        String groupId = topologyName;
        if(commandLine.hasOption('g') ) {
            groupId = commandLine.getOptionValue('g');
        }
        
        boolean fromStart = false;
        if(commandLine.hasOption('S') ) {  
            fromStart = true; 
        } 

        int workernum = 3;
        if(commandLine.hasOption('w') ) {  
            workernum = Integer.parseInt(commandLine.getOptionValue('w'));  
        } 

        int tasknum = 3;
        if(commandLine.hasOption('T') ) {  
            tasknum = Integer.parseInt(commandLine.getOptionValue('T'));  
        } 

        boolean localmode = false;
        if(commandLine.hasOption('l') ) {  
            localmode = true;  
        } 

        String streamargs = "";
        if(commandLine.hasOption('s') ) {  
            streamargs = commandLine.getOptionValue('s');  
        } 
         
        RtAnalytics rta = new RtAnalytics(topic);
        rta.kafkaSpout(groupId, topic, zkServers, offsetZkRoot, fromStart);
        rta.hdfsBolt(namenode, path, nnhaConfig);
        try {
           rta.runTopology(topologyName, tasknum, workernum, localmode, streamargs);
        } catch(AlreadyAliveException e) {
           System.out.println("[!!!] AlreadyAliveException");
        } catch(InvalidTopologyException e) {
           System.out.println("[!!!] InvalidTopologyException");
        } catch(InterruptedException e) {
           System.out.println("[!!!] InterruptedException");
        }
    }
}

