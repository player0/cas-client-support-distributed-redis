package com.wangbin.cas.client.proxy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.jasig.cas.client.proxy.AbstractEncryptedProxyGrantingTicketStorageImpl;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

public class RedisBackedProxyGrantingTicketStorageImpl extends AbstractEncryptedProxyGrantingTicketStorageImpl {

	private RedisClusterClient client;

	private RedisAdvancedClusterAsyncCommands<String, String> commands;

	private String namespace;

	/**
	 * Default constructor reads from the /casclient_redis_hosts.txt in the
	 * classpath. Each line should be a host:port combination of redis servers.
	 */
	public RedisBackedProxyGrantingTicketStorageImpl() {
		this(getHostsFromClassPath());
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	protected static String[] getHostsFromClassPath() {
		final InputStream inputStream = RedisBackedProxyGrantingTicketStorageImpl.class
				.getResourceAsStream("/casclient_redis_hosts.txt");
		final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		final List<String> hosts = new ArrayList<String>();

		String line;
		try {
			while ((line = reader.readLine()) != null) {
				hosts.add(line);
			}

			return hosts.toArray(new String[hosts.size()]);
		} catch (final IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				reader.close();
			} catch (final IOException e) {
				// do nothing
			}

			try {
				inputStream.close();
			} catch (final IOException e) {
				// do nothing
			}
		}
	}

	public RedisBackedProxyGrantingTicketStorageImpl(final String[] hostnamesAndPorts) {
		final List<RedisURI> uris = new ArrayList<RedisURI>();

		for (final String hostname : hostnamesAndPorts) {
			final String[] hostPort = hostname.split(":");
			uris.add(RedisURI.create(hostPort[0], Integer.parseInt(hostPort[1])));
		}

		this.client = RedisClusterClient.create(uris);
		this.commands = client.connect().async();
	}

	@Override
	protected void saveInternal(String proxyGrantingTicketIou, String proxyGrantingTicket) {
		commands.set(namespace + proxyGrantingTicketIou, proxyGrantingTicket);
	}

	@Override
	protected String retrieveInternal(String proxyGrantingTicketIou) {
		RedisFuture<String> future = commands.get(namespace + proxyGrantingTicketIou);
		try {
			return future.get();
		} catch (Exception e) {
			return null;
		}
	}

	public void cleanUp() {
		// do nothing
	}
}
