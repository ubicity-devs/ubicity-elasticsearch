package at.ac.ait.ubicity.ubicity.elasticsearch.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Stack;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import at.ac.ait.ubicity.commons.broker.events.ESMetadata;
import at.ac.ait.ubicity.commons.broker.events.ESMetadata.Properties;
import at.ac.ait.ubicity.commons.broker.events.EventEntry;
import at.ac.ait.ubicity.commons.broker.events.Metadata;
import at.ac.ait.ubicity.commons.util.PropertyLoader;
import at.ac.ait.ubicity.core.Core;
import at.ac.ait.ubicity.ubicity.elasticsearch.ESClient;
import at.ac.ait.ubicity.ubicity.elasticsearch.ElasticsearchAddOn;

@PluginImplementation
public class ElasticsearchAddOnImpl implements ElasticsearchAddOn, Runnable {

	private final String name;

	private final ESClient client;
	private final HashSet<String> knownIndizes = new HashSet<String>();
	private final Core core;

	private final int uniqueId;

	private static int BULK_SIZE;
	private static int BULK_TIMEOUT;

	protected static Logger logger = Logger
			.getLogger(ElasticsearchAddOnImpl.class);

	private boolean shutdown = false;
	Stack<IndexRequest> requests = new Stack<IndexRequest>();

	public ElasticsearchAddOnImpl() {
		uniqueId = new Random().nextInt();

		PropertyLoader config = new PropertyLoader(
				ElasticsearchAddOnImpl.class.getResource("/elasticsearch.cfg"));

		this.name = config.getString("addon.elasticsearch.name");
		BULK_SIZE = config.getInt("addon.elasticsearch.bulk_size");
		BULK_TIMEOUT = config.getInt("addon.elasticsearch.bulk_timeout");

		String server = config.getString("addon.elasticsearch.host");
		int port = config.getInt("addon.elasticsearch.host_port");
		String cluster = config.getString("addon.elasticsearch.cluster");

		client = new ESClient(server, port, cluster);

		core = Core.getInstance();
		core.register(this);

		Thread t = new Thread(this);
		t.setName("execution context for " + getName());
		t.start();
	}

	@Override
	public final int hashCode() {
		return uniqueId;
	}

	@Override
	public final boolean equals(Object o) {

		if (ElasticsearchAddOnImpl.class.isInstance(o)) {
			ElasticsearchAddOnImpl other = (ElasticsearchAddOnImpl) o;
			return other.uniqueId == this.uniqueId;
		}
		return false;
	}

	public void onEvent(EventEntry event, long sequence, boolean endOfBatch)
			throws Exception {

		if (event != null) {
			// shutdown addon
			if (event.isPoisoned()) {
				logger.info("ConsumerPoison received");
				shutdown();

				return;
			}

			// logger.info("Sending took: [ms] " + (System.currentTimeMillis() -
			// event.getCreatedTs()));

			ESMetadata meta = getMyConfiguration(event.getCurrentMetadata());

			String esIdx = meta.getProperties().get(
					Properties.ES_INDEX.toString());
			String esType = meta.getProperties().get(
					Properties.ES_TYPE.toString());

			// Check if Idx exists otherwise create it
			if (!knownIndizes.contains(esIdx) && !client.indexExists(esIdx)) {
				client.createIndex(esIdx);
				knownIndizes.add(esIdx);
			}

			IndexRequest ir = new IndexRequest(esIdx, esType);
			ir.id(event.getId());
			ir.source(event.getData());

			requests.push(ir);
		}
	}

	private ESMetadata getMyConfiguration(List<Metadata> data) {

		for (Metadata d : data) {
			if (this.name.equals(d.getDestination())
					&& ESMetadata.class.isInstance(d)) {
				return (ESMetadata) d;
			}
		}

		return null;
	}

	public String getName() {
		return name;
	}

	private void closeConnections() {
		client.close();
	}

	public boolean shutdown() {
		shutdown = true;
		core.deRegister(this);
		return true;
	}

	public void run() {

		long startTime = System.currentTimeMillis();

		while (!shutdown) {
			try {

				Thread.sleep(100);

				if (requests.size() > BULK_SIZE
						|| (System.currentTimeMillis() - startTime > BULK_TIMEOUT && requests
								.size() > 0)) {

					BulkRequestBuilder bulk = client.getBulkRequestBuilder();

					synchronized (requests) {
						while (!requests.isEmpty()) {
							bulk.add(requests.pop());
						}
					}

					BulkResponse resp = bulk.get();

					if (resp.hasFailures()) {
						logger.warn("Bulk request failed with "
								+ resp.buildFailureMessage());
					}

					startTime = System.currentTimeMillis();
				}

			} catch (InterruptedException e) {
				;
			}
		}
		shutdown();
		closeConnections();
	}
}
