package net.iponweb.disthene.remover;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.function.Consumer;

@SuppressWarnings("deprecation")
public class IndexService {

    private static final String INDEX_NAME = "disthene";

    protected final RestHighLevelClient client;
    protected final String tenant;

    public IndexService(String host, String tenant) {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, 9200))
                        .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(14_400_000))
        );

        this.tenant = tenant;
    }

    public void process(String wildcard, Consumer<String> action) throws IOException {
        final String regEx = WildcardUtil.getPathsRegExFromWildcard(wildcard);

        final Scroll scroll = new Scroll(TimeValue.timeValueHours(4L));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource("path", null)
                .size(10_000)
                .query(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.regexpQuery("path.keyword", regEx))
                                .filter(QueryBuilders.termQuery("tenant.keyword", tenant))
                                .filter(QueryBuilders.termQuery("leaf", true))
                );

        SearchRequest request = new SearchRequest(INDEX_NAME)
                .source(sourceBuilder)
                .scroll(scroll);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();

        SearchHits hits = response.getHits();

        while (hits.getHits().length > 0) {
            for (SearchHit hit : hits) {
                String path = String.valueOf(hit.getSourceAsMap().get("path"));
                action.accept(path);
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            hits = response.getHits();
        }
    }

    public void deleteByWildCard(String wildcard) throws IOException {
        final String regEx = WildcardUtil.getPathsRegExFromWildcard(wildcard);

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(INDEX_NAME)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.regexpQuery("path.keyword", regEx))
                        .filter(QueryBuilders.termQuery("tenant.keyword", tenant))
                        .filter(QueryBuilders.termQuery("leaf", true))
                )
                .setScroll(TimeValue.timeValueHours(4L))
                .setTimeout(TimeValue.timeValueHours(4L));

        client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }

    public void removeTenant() throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(INDEX_NAME)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("tenant", tenant))
                )
                .setScroll(TimeValue.timeValueHours(4L))
                .setTimeout(TimeValue.timeValueHours(4L));

        client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
    }

    public void close() throws IOException {
        client.close();
    }
}
