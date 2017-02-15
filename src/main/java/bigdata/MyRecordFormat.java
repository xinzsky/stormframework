package bigdata;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.storm.hdfs.bolt.format.RecordFormat;


public class MyRecordFormat implements RecordFormat {
    public static final String DEFAULT_FIELD_DELIMITER = ",";
    public static final String DEFAULT_RECORD_DELIMITER = "\n";
    private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private String recordDelimiter = DEFAULT_RECORD_DELIMITER;
    private Fields fields = null;
    private String excludeField = "";

    /**
     * Only output the specified fields.
     *
     * @param fields
     * @return
     */
    public MyRecordFormat withFields(Fields fields){
        this.fields = fields;
        return this;
    }

    /**
     * Overrides the default field delimiter.
     *
     * @param delimiter
     * @return
     */
    public MyRecordFormat withFieldDelimiter(String delimiter){
        this.fieldDelimiter = delimiter;
        return this;
    }

    /**
     * Overrides the default record delimiter.
     *
     * @param delimiter
     * @return
     */
    public MyRecordFormat withRecordDelimiter(String delimiter){
        this.recordDelimiter = delimiter;
        return this;
    }

    public MyRecordFormat excludeField(String field) {
        this.excludeField = field;
        return this;
    }

    @Override
    public byte[] format(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        Fields fields = this.fields == null ? tuple.getFields() : this.fields;
        int size = fields.size();
        for(int i = 0; i < size; i++) {
            if(excludeField.equals("") || !excludeField.equals(fields.get(i))) {
                sb.append(tuple.getValueByField(fields.get(i)));
                if(i == size -2 && !excludeField.equals("") && excludeField.equals(fields.get(i+1))) {
                    continue;
                } else if(i != size - 1) {
                    sb.append(this.fieldDelimiter);
                }
            }
        }
        sb.append(this.recordDelimiter);
        return sb.toString().getBytes();
    }
}
