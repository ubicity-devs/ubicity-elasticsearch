package at.ac.ait.ubicity.ubicity.elasticsearch.impl;

import java.util.HashSet;

import net.xeoh.plugins.base.annotations.PluginImplementation;
import net.xeoh.plugins.base.annotations.events.Init;
import net.xeoh.plugins.base.annotations.events.Shutdown;

import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import at.ac.ait.ubicity.commons.broker.BrokerConsumer;
import at.ac.ait.ubicity.commons.broker.events.EventEntry;
import at.ac.ait.ubicity.commons.broker.events.EventEntry.Property;
import at.ac.ait.ubicity.commons.broker.exceptions.UbicityBrokerException;
import at.ac.ait.ubicity.commons.util.PropertyLoader;
import at.ac.ait.ubicity.ubicity.elasticsearch.ESClient;
import at.ac.ait.ubicity.ubicity.elasticsearch.ElasticsearchPlugin;

@PluginImplementation
public class ElasticsearchPluginImpl extends BrokerConsumer implements
		ElasticsearchPlugin {

	private String name;

	private static ESClient client;
	private final HashSet<String> knownIndizes = new HashSet<String>();

	private static int BULK_SIZE;
	private static int BULK_FLUSH_MS;

	protected static Logger logger = Logger
			.getLogger(ElasticsearchPluginImpl.class);

	private BulkProcessor bulkProcessor;

	@Init
	public void init() {
		PropertyLoader config = new PropertyLoader(
				ElasticsearchPluginImpl.class.getResource("/elasticsearch.cfg"));

		try {
			super.init(config.getString("plugin.elasticsearch.broker.user"),
					config.getString("plugin.elasticsearch.broker.pwd"));

			this.name = config.getString("plugin.elasticsearch.name");
			BULK_SIZE = config.getInt("plugin.elasticsearch.bulk_size");
			BULK_FLUSH_MS = config.getInt("plugin.elasticsearch.bulk_flush_ms");

			String server = config.getString("plugin.elasticsearch.host");
			int port = config.getInt("env.elasticsearch.host_port");
			String cluster = config.getString("plugin.elasticsearch.cluster");
			client = new ESClient(server, port, cluster);

			bulkProcessor = client.getBulkProcessor(BULK_SIZE, BULK_FLUSH_MS);

			setConsumer(this,
					config.getString("plugin.elasticsearch.broker.dest"));

			logger.info(name + " loaded");

		} catch (UbicityBrokerException e) {
			logger.error("During init caught exc.", e);
		}
	}

	@Override
	public void onReceived(String destination, EventEntry event) {

		if (event != null) {

			String esIdx = event.getHeader().get(Property.ES_INDEX);
			String esType = event.getHeader().get(Property.ES_TYPE);
			String id = event.getHeader().get(Property.ID);

			// Check if Idx exists otherwise create it
			if (!knownIndizes.contains(esIdx) && !client.indexExists(esIdx)) {
				client.createIndex(esIdx);
				knownIndizes.add(esIdx);
			}

			IndexRequest ir = new IndexRequest(esIdx, esType);
			ir.id(id);
			ir.source(event.getBody());

			bulkProcessor.add(ir);
		}
	}

	@Override
	protected void onReceivedRaw(String destination, String tmsg) {
		// Not used here
	}

	public String getName() {
		return name;
	}

	private void closeConnections() {
		bulkProcessor.close();
		client.close();
	}

	@Override
	@Shutdown
	public void shutdown() {
		closeConnections();
		super.shutdown();
	}
}
