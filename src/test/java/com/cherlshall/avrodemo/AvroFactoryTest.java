package com.cherlshall.avrodemo;

import com.cherlshall.avrodemo.avrodemo.AvroFactory;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hu.tengfei
 * @date 2019/8/9
 */
public class AvroFactoryTest {

    @Test
    public void test() throws IOException {
        String schema = "{" +
                "   \"type\": \"record\"," +
                "   \"name\": \"user\"," +
                "   \"fields\": [" +
                "       {" +
                "           \"name\": \"name\"," +
                "           \"type\": [\"null\", \"string\"]" +
                "       }," +
                "       {" +
                "           \"name\": \"age\"," +
                "           \"type\": [\"null\", \"int\"]" +
                "       }," +
                "       {" +
                "           \"name\": \"gender\"," +
                "           \"type\": [\"null\", \"boolean\"]" +
                "       }," +
                "       {" +
                "           \"name\": \"info\"," +
                "           \"type\": [\"null\", \"bytes\"]" +
                "       }" +
                "   ]" +
                "}";
        Map<String, Object> data = new HashMap<>();
        data.put("name", "xiaoming");
        data.put("age", 18);
        data.put("gender", true);
        data.put("info", ByteBuffer.wrap(new byte[] {1,2,3}));
        byte[] bytes = AvroFactory.mapToByte(schema, data);
        GenericRecord record = AvroFactory.byteToRecord(schema, bytes);
        System.out.println(Arrays.toString(bytes));
        System.out.println(record);
    }
}
