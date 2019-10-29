package org.elasticsearch.plugin.bitmap;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;

/**
 * @author : zhanpenghong
 * @date : 2019-10-18 11:44
 */
public class BitmapAggregationPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return Collections.singletonList(new AggregationSpec(BitmapAggregationBuilder.NAME, BitmapAggregationBuilder::new, BitmapAggregationBuilder::parse)
                .addResultReader(InternalBitmap::new));
    }
}
