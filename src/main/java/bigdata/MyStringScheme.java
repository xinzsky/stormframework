package bigdata;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class MyStringScheme implements Scheme {
    public static final String STRING_SCHEME_KEY = "key";
    public static final String STRING_SCHEME_VALUE = "value";
    public static final String KV_DELIMITER = "ox^^xo";

    public List<Object> deserialize(byte[] bytes) {
        String value = deserializeString(bytes);
        int pos = value.indexOf(KV_DELIMITER, 0);
        if(pos == -1) {
          return new Values("", value);
        } else {
          String key = value.substring(0, pos);
          value = value.substring(pos+KV_DELIMITER.length(), value.length());
          return new Values(key, value);
        } 
    }

    public static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }        
    }

    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY, STRING_SCHEME_VALUE);
    }
}
