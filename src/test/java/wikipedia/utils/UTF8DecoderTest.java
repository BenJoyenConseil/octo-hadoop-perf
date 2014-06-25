package wikipedia.utils;

import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.Charset;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class UTF8DecoderTest {

    @Test
    public void unescape_ShouldReplaceSpecialCharacters() throws Exception {
        // Given
        String pageNameUnFormated = "jean-fran%c3%a7ois cop%c3%a9";
        String expected = "jean-françois copé";

        // When
        String result = UTF8Decoder.unescape(pageNameUnFormated);

        // Then
        System.out.println(result);
        assertThat(result, is(equalTo(expected)));
    }


    @Test
    public void unescape_ShouldExpulsePourcentAtTheEndOfString() throws Exception {
        // Given
        String pageNameUnFormated = "Albert_Einstein%";
        String expected = "Albert_Einstein%";

        // When
        String result = UTF8Decoder.unescape(pageNameUnFormated);

        // Then
        assertThat(result, is(equalTo(expected)));
    }

    @Test
    public void unescape_ShouldNotThrowException_WhenEndOfWordIsEqualToPercent() throws Exception {
        // Given
        String pageNameUnFormated = "absolute%20-29.4%";
        String expected = "absolute -29.4%";

        // When
        String result = UTF8Decoder.unescape(pageNameUnFormated);

        // Then
        assertThat(result, is(equalTo(expected)));
    }
}
