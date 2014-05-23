package at.ac.ait.ubicity.ubicity.elasticsearch.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import at.ac.ait.ubicity.commons.broker.events.ESMetadata;
import at.ac.ait.ubicity.commons.broker.events.ESMetadata.Properties;
import at.ac.ait.ubicity.commons.broker.events.EventEntry;
import at.ac.ait.ubicity.commons.broker.events.Metadata;
import at.ac.ait.ubicity.core.Core;
import at.ac.ait.ubicity.ubicity.elasticsearch.ESClient;
import at.ac.ait.ubicity.ubicity.elasticsearch.ElasticsearchAddOn;

@PluginImplementation
public class ElasticsearchAddOnImpl implements ElasticsearchAddOn {

	private String name;

	private ESClient client;
	private BulkRequestBuilder bulk;
	private final HashSet<String> knownIndizes = new HashSet<String>();

	private long startTime;

	private final Core core;

	private final int uniqueId;

	private static int BULK_SIZE;
	private static int BULK_TIMEOUT;

	protected static Logger logger = Logger
			.getLogger(ElasticsearchAddOnImpl.class);

	public ElasticsearchAddOnImpl() {
		uniqueId = new Random().nextInt();

		try {
			// set necessary stuff for us to ueberhaupt be able to work
			Configuration config = new PropertiesConfiguration(
					ElasticsearchAddOnImpl.class
							.getResource("/elasticsearch.cfg"));

			this.name = config.getString("addon.elasticsearch.name");
			BULK_SIZE = config.getInt("addon.elasticsearch.bulk_size");
			BULK_TIMEOUT = config.getInt("addon.elasticsearch.bulk_timeout");

			String server = config.getString("addon.elasticsearch.host");
			int port = config.getInt("addon.elasticsearch.host_port");
			String cluster = config.getString("addon.elasticsearch.cluster");

			client = new ESClient(server, port, cluster);

		} catch (ConfigurationException noConfig) {
			logger.fatal("Configuration not found! " + noConfig.toString());
			throw new RuntimeException();
		}

		init();

		core = Core.getInstance();
		core.register(this);
	}

	private void init() {

		bulk = client.getBulkRequestBuilder();
		startTime = System.currentTimeMillis();
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
				closeConnections();
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

			bulk.add(ir);

			// FIXME: Means that without constant stream requests might not be
			// send in acceptable time
			if (bulk.numberOfActions() > BULK_SIZE
					|| System.currentTimeMillis() - startTime > BULK_TIMEOUT) {

				BulkResponse resp = bulk.get();

				if (resp.hasFailures()) {
					logger.warn("Bulk request failed with "
							+ resp.buildFailureMessage());
				}

				startTime = System.currentTimeMillis();
			}
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
		core.deRegister(this);
		return true;
	}
}
