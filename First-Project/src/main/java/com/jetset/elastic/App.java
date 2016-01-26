package com.jetset.elastic;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		System.out.println("Hello World!");
		Node node = nodeBuilder().clusterName("elasticsearch").node();
		Client client = node.client();

		//deleteDocument(client, "kodcucom", "article", "1");
		//deleteDocument(client, "kodcucom", "article", "2");
		
		client.prepareIndex("datacafe", "es", "1")
				.setSource(putJsonDocument("This is the first entry as Tag",
						"Inserting few entries in the given ES topics",
						new Date(), new String[] { "datacafe", "anurag", "elasticsearch" }, "Anurag Jain"))
				.execute().actionGet();

		client.prepareIndex("datacafe", "es", "2")
				.setSource(putJsonDocument("This is the second entry as Tag",
						"Inserting another entry in the given ES topics",
						new Date(), new String[] { "datacafe", "java", "examples" }, "Anurag Jain"))
				.execute().actionGet();


		client.admin().indices().prepareRefresh().get();

		System.out.println("********************************getting document********************************");
		getDocument(client, "datacafe", "es", "1");

		System.out.println("********************************searching document********************************");
		//searchDocument(client, "kodcucom", "article", "title", "anurag");
		
		String[] tags = {"tags", "tags"};
		String[] values = {"datacafe", "anurag"};
		
		multiSearchDocument(client, "datacafe", "es", tags, values);

		System.out.println("********************************deleting document********************************");
		deleteDocument(client, "datacafe", "es", "1");
		deleteDocument(client, "datacafe", "es", "2");

		node.close();
	}

	public static Map<String, Object> putJsonDocument(String title, String content, Date postDate, String[] tags,
			String author) {

		Map<String, Object> jsonDocument = new HashMap<String, Object>();

		jsonDocument.put("title", title);
		jsonDocument.put("content", content);
		jsonDocument.put("postDate", postDate);
		jsonDocument.put("tags", tags);
		jsonDocument.put("author", author);

		return jsonDocument;
	}

	public static void getDocument(Client client, String index, String type, String id) {

		GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();
		Map<String, Object> source = getResponse.getSource();

		System.out.println("------------------------------");
		System.out.println("Index: " + getResponse.getIndex());
		System.out.println("Type: " + getResponse.getType());
		System.out.println("Id: " + getResponse.getId());
		System.out.println("Version: " + getResponse.getVersion());
		System.out.println(source);
		System.out.println("------------------------------");

	}

	public static void updateDocument(Client client, String index, String type, String id, String field,
			String newValue) {

		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, newValue);

		client.prepareUpdate(index, type, id)
				.setScript(new Script("ctx._source.title = \"ElasticSearch: Java API\"", ScriptType.INLINE, null, null))
				.get();
	}

	public static void multiSearchDocument(Client client, String index, String type, String[] fields, String[] values) {
		
		BoolQueryBuilder bqb = 
				QueryBuilders.boolQuery();
		/*		.must(QueryBuilders.termQuery("tags", "datacafe"))
				.must(QueryBuilders.termQuery("tags", "java"))
				.minimumShouldMatch("100");*/
		for(int i=0; i < fields.length; i++) {
			bqb.must(QueryBuilders.termQuery(fields[i], values[i]));
		}
		
		bqb.minimumShouldMatch("100");
		
		SearchResponse response = client.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.DEFAULT)
				.setQuery(bqb)
				.setFrom(0).setSize(60)
				.setExplain(false)
				.execute()
				.actionGet();
		System.out.println("anurag jain ******" + response.getHits().totalHits());
		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}
	}
	
	public static void searchDocument(Client client, String index, String type, String field, String value) {

		SearchResponse response = client.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.DEFAULT)
				.setQuery(QueryBuilders.termQuery(field, value))
				.setFrom(0).setSize(60)
				.setExplain(true)
				.execute()
				.actionGet();
		System.out.println("anurag jain ******" + response.getHits().totalHits());
		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}
	}

	public static void deleteDocument(Client client, String index, String type, String id) {

		DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
		System.out.println("Information on the deleted document:");
		System.out.println("Index: " + response.getIndex());
		System.out.println("Type: " + response.getType());
		System.out.println("Id: " + response.getId());
		System.out.println("Version: " + response.getVersion());
	}
}
