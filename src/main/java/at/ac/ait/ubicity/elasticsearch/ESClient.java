package at.ac.ait.ubicity.elasticsearch;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import at.ac.ait.ubicity.commons.util.UbicityException;

public class ESClient {

	private final Client client;

	private static final Logger logger = Logger.getLogger(ESClient.class);

	public ESClient(String server, int port, String cluster) {

		Thread.currentThread().setContextClassLoader(
				ESClient.class.getClassLoader());
		// instantiate an elasticsearch client
		Settings settings = ImmutableSettings
				.settingsBuilder()
				// .put("cluster.name", cluster)
				.put("client.transport.ignore_cluster_name", false)
				.put("client.transport.nodes_sampler_interval", "30s")
				.put("client.transport.ping_timeout", "30s").build();
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
	 * @throws UbicityException
	 */
	public void createIndex(String idx) throws UbicityException {
		try {
			CreateIndexRequestBuilder createIndexRequestBuilder = client
					.admin().indices().prepareCreate(idx);
			createIndexRequestBuilder.execute().actionGet();
		} catch (Exception e) {
			throw new UbicityException(e);
		}
	}

	/**
	 * Checks if the given Index exists.
	 * 
	 * @param idx
	 * @throws UbicityException
	 */
	public boolean indexExists(String idx) throws UbicityException {

		try {
			ActionFuture<IndicesExistsResponse> resp = client.admin().indices()
					.exists(new IndicesExistsRequest(idx));
			return resp.get().isExists();
		} catch (Exception e) {
			throw new UbicityException(e);
		}
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

	public BulkProcessor getBulkProcessor(int bulkSize, long timeIntervalMs) {

		BulkProcessor bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {

					public void beforeBulk(long executionId, BulkRequest request) {
					}

					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
					}

					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
					}
				}).setBulkActions(bulkSize)
				.setFlushInterval(TimeValue.timeValueMillis(timeIntervalMs))
				.build();

		return bulkProcessor;
	}

}
