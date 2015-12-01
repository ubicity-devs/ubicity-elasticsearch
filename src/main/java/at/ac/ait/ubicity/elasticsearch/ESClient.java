package at.ac.ait.ubicity.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import at.ac.ait.ubicity.commons.exceptions.UbicityException;

public class ESClient {

	private final TransportClient client;

	private static final Logger logger = Logger.getLogger(ESClient.class);

	public ESClient(String[] serverList, int port) {
		Thread.currentThread().setContextClassLoader(ESClient.class.getClassLoader());

		client = TransportClient.builder().build();

		for (int i = 0; i < serverList.length; i++) {
			try {
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(serverList[i]), port));
			} catch (UnknownHostException e) {
				logger.error("ES Hostname not found: " + e);
			}
		}

		logger.info("Connected to " + Arrays.toString(serverList) + ":" + port);
	}

	/**
	 * Creates the given Index if it does not exist.
	 * 
	 * @param idx
	 * @throws UbicityException
	 */
	public void createIndex(String idx) throws UbicityException {
		try {
			CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(idx);
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
			ActionFuture<IndicesExistsResponse> resp = client.admin().indices().exists(new IndicesExistsRequest(idx));
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

		BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			}
		}).setBulkActions(bulkSize).setFlushInterval(TimeValue.timeValueMillis(timeIntervalMs)).build();

		return bulkProcessor;
	}

}
