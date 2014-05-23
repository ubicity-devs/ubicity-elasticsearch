package at.ac.ait.ubicity.ubicity.elasticsearch;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClient {

	private final Client client;

	private static final Logger logger = Logger.getLogger(ESClient.class);

	public ESClient(String server, int port, String cluster) {

		// instantiate an elasticsearch client
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", cluster).build();

		client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(server,
						port));

		logger.info("Connected to " + server + ":" + port + " - Cluster: "
				+ cluster);
	}

	/**
	 * Creates the given Index if it does not exist.
	 * 
	 * @param idx
	 */
	public void createIndex(String idx) {
		try {
			CreateIndexRequestBuilder createIndexRequestBuilder = client
					.admin().indices().prepareCreate(idx);
			createIndexRequestBuilder.execute().actionGet();
		} catch (Exception e) {
			// Throws exc. if index already exists
			logger.warn("ES returned error: " + e.getMessage());
		}
	}

	/**
	 * Checks if the given Index exists.
	 * 
	 * @param idx
	 */
	public boolean indexExists(String idx) {

		try {
			ActionFuture<IndicesExistsResponse> resp = client.admin().indices()
					.exists(new IndicesExistsRequest(idx));
			return resp.get().isExists();
		} catch (Exception e) {
			logger.error("ES returned error: " + e.getMessage());
		}
		return false;
	}

	public IndexRequestBuilder getSingleRequestBuilder() {
		return client.prepareIndex();
	}

	public BulkRequestBuilder getBulkRequestBuilder() {
		return client.prepareBulk();
	}

	public void close() {
		client.close();
	}

}
