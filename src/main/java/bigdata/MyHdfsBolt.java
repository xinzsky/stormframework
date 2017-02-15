package bigdata;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import java.io.IOException;
import java.util.Map;
import java.io.BufferedInputStream; 
import java.io.FileInputStream; 
import java.io.FileNotFoundException; 
import java.io.FileOutputStream; 
import java.io.IOException; 
import java.io.InputStream; 
import java.io.OutputStream; 
import java.util.Properties; 


public class MyHdfsBolt extends HdfsBolt  
{
    private String nnhaConfig = "";
   
    public MyHdfsBolt(String nnhaConfig) {
       super();
       this.nnhaConfig = nnhaConfig;
    }

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        if(!nnhaConfig.equals("")) {
            String key, value;
            Properties props = new Properties(); 
            System.out.println("[MyHdfsBolt] Namenode HA config: " + nnhaConfig);
            try { 
                InputStream in = new BufferedInputStream(new FileInputStream(nnhaConfig)); 
                props.load(in); 

                key = "dfs.nameservices";
                value = props.getProperty(key);
                this.hdfsConfig.set(key, value);

                String nameservices = value;
                key = "dfs.ha.namenodes." + nameservices;
                value = props.getProperty(key);
                this.hdfsConfig.set(key, value);

                String [] namenodes = value.split(",");
                for(int i=0; i < namenodes.length; i++) {
                   key = "dfs.namenode.rpc-address." + nameservices + "." + namenodes[i].trim(); 
                   value = props.getProperty(key);
                   this.hdfsConfig.set(key, value);
                }
                
                key = "dfs.client.failover.proxy.provider." + nameservices;
                value = props.getProperty(key);
                this.hdfsConfig.set(key, value);
            } catch (Exception e) { 
                System.out.println("[MyHdfsBolt] Namenode HA config error.");
            } 
        }
        
        super.doPrepare(conf, topologyContext, collector);
    }
}
