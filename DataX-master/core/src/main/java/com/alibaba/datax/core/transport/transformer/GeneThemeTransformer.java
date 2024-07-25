package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kwk on 23/10/15
 * 根据url找对应搜索词
 * 自定义位置
 */
public class GeneThemeTransformer extends Transformer {

    public GeneThemeTransformer() {
        setTransformerName("dx_genetheme");
    }

    public static Map<String, String> extractQueryParameters(String url) {
        Map<String, String> queryParams = new HashMap<>();

        try {
            String query = url.substring(url.indexOf('?') + 1);
            String[] pairs = query.split("&");

            for (String pair : pairs) {
                int equalsIndex = pair.indexOf('=');
                if (equalsIndex >= 0) {
                    String key = pair.substring(0, equalsIndex);
                    String value = pair.substring(equalsIndex + 1);
                    queryParams.put(key, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return queryParams;
    }

    public static String decodePercentString(String encodedString) {
        try {
            String decodedString = URLDecoder.decode(encodedString, "UTF-8");
            return decodedString;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int UrlIndex;
        int ThemeIndex;
        try {
            if (paras.length != 2) {
                throw new Exception("paras length is not 2");
            }

            UrlIndex = (Integer) paras[0];
            ThemeIndex = Integer.valueOf((String) paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras: " + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column url = record.getColumn(UrlIndex);
        try {
            String oriValue = url.asString();

            if (oriValue == null) {
                return record;
            }

            Map<String, String> queryParams = extractQueryParameters(oriValue);
            String searchItem = queryParams.get("q");
            String theme = decodePercentString(searchItem);
            record.setColumn(ThemeIndex, new StringColumn(theme));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
