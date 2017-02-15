package bigdata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Streaming 
{
	private static final Log LOG = LogFactory.getLog(Streaming.class);
        private String args;

	public void open(String args) 
	{
            //System.out.println("streaming open....");
            this.args = args;
	}

	public String process(String key, String message) 
	{
            LOG.info("[Streaming] key: " + key + " message: " + message);
            // add you code ...
            return message;
	}

	public void close() 
	{
            //System.out.println("streaming close....");
	}
}
