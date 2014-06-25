package wikipedia.mappers;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import wikipedia.domain.PageNameAndCount;
import wikipedia.reducers.SimpleReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SimpleMapperTest {

    SimpleMapper logMapper = new SimpleMapper();

    MapDriver<LongWritable, Text, Text, PageNameAndCount> mapDriver;
    ReduceDriver<Text, PageNameAndCount, Text, PageNameAndCount> reduceDriver;

    @Before
    public void setup(){
        mapDriver = MapDriver.newMapDriver(logMapper);
        reduceDriver = ReduceDriver.newReduceDriver(new SimpleReducer());
        mapDriver.addCacheFile("conf/page_names_to_skip.txt");
    }

    @Test
    public void isRecordToBeIgnored_shouldReturnTrue_WhenPageNameDoesNotMatchTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("Special");
        list.add("Undefined");
        logMapper.restrictions = list;
        String pageName = "undefined bluck";

        // Then
        boolean result = logMapper.isRecordToBeIgnored(pageName);

        // When
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void isRecordToBeIgnored_shouldReturnFalse_WhenPageNameMatchesTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("Special");
        list.add("undefined");
        logMapper.restrictions = list;
        String pageName = "bluckblcuk";

        // Then
        boolean result = logMapper.isRecordToBeIgnored(pageName);

        // When
        assertThat(result, is(equalTo(false)));
    }
}
