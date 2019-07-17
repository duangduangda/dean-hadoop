package org.dean.flink.transformation;

import com.google.common.io.Resources;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;

/**
 * @description: 参数化配置
 * @author: dean
 * @create: 2019/07/17 15:33
 */
public class ParameterToolTest {

    @Test
    public void testParameterTool() throws IOException {
        InputStream inputStream = Resources.getResource("flink.properties").openStream();
        ParameterTool tool = ParameterTool.fromPropertiesFile(inputStream);
        assertEquals("1", tool.get("parallems"));
    }
}
