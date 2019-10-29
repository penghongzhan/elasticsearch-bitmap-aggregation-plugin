package org.elasticsearch.plugin.bitmap;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;

/**
 * @author : zhanpenghong
 * @date : 2019-10-28 20:08
 */
public class ParsedBitmap extends ParsedSingleValueNumericMetricsAggregation implements BitmapDistinct {

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public String getType() {
        return BitmapAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), value);
        if (valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    private static final ObjectParser<ParsedBitmap, Void> PARSER = new ObjectParser<>(ParsedBitmap.class.getSimpleName(),
            true, ParsedBitmap::new);

    static {
        declareSingleValueFields(PARSER, Double.NEGATIVE_INFINITY);
    }

    public static ParsedBitmap fromXContent(XContentParser parser, final String name) {
        ParsedBitmap sum = PARSER.apply(parser, null);
        sum.setName(name);
        return sum;
    }
}
