package com.mzd.myelasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MyElasticsearchApplicationTests {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void contextLoads() {
        Map<String, Object> criteria = new HashMap<>();
        criteria.put("name", "测试");

        //search request
        SearchRequest searchRequest = new SearchRequest("test");

        //search builder
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder booleanQueryBuilder = QueryBuilders.boolQuery();
        for (String key : criteria.keySet()) {
            booleanQueryBuilder.must(QueryBuilders.matchQuery(key, criteria.get(key)));
        }

        sourceBuilder.query(booleanQueryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //sort
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        //highlight
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("text");
        highlightTitle.preTags("<span class=\"highlight\">");
        highlightTitle.postTags("</span>");
        highlightBuilder.field(highlightTitle);
        sourceBuilder.highlighter(highlightBuilder);

        // add builder into request
        searchRequest.indices("test");
        searchRequest.source(sourceBuilder);

        //response
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //search hits
            SearchHits hits = searchResponse.getHits();
            System.out.println(JSON.toJSONString(hits));
            TotalHits totalHits = hits.getTotalHits();
            System.out.println("查询出来个数：" + totalHits.value);

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String str = hit.getSourceAsString();
                System.out.println("查询结果为：" + str);
                //高亮
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                HighlightField highlight = highlightFields.get("时间");
                if (highlight != null) {
                    Text[] fragments = highlight.fragments();
                    String fragmentString = fragments[0].string();
                    System.out.println("高亮后：" + fragmentString);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
