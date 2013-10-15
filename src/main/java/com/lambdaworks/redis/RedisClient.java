// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis;

import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.Utf8StringCodec;
import com.lambdaworks.redis.protocol.*;
import com.lambdaworks.redis.pubsub.PubSubCommandHandler;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.*;

/**
 * A scalable thread-safe <a href="http://redis.io/">Redis</a> client. Multiple threads
 * may share one connection provided they avoid blocking and transactional operations
 * such as BLPOP and MULTI/EXEC.
 *
 * @author Will Glozer
 */
public class RedisClient {
    private ClientBootstrap bootstrap;
    private Timer timer;
    private ChannelGroup channels;
    private long timeout;
    private TimeUnit unit;
    private IRedisConnectionStateListener connectionStateListener = null;

    /**
     * Create a new client that connects to the supplied host on the default port.
     *
     * @param host    Server hostname.
     */
    public RedisClient(String host) {
        this(host, 6379);
    }
    
    /**
     * Create a new client that connects to the supplied host on the default port.
     *
     * @param host    Server hostname.
     */
    public RedisClient(String host, IRedisConnectionStateListener listener) {
        this(host, 6379);
        this.connectionStateListener = listener;
    }

    /**
     * Create a new client that connects to the supplied host and port. Connection
     * attempts and non-blocking commands will {@link #setDefaultTimeout timeout}
     * after 60 seconds.
     *
     * @param host    Server hostname.
     * @param port    Server port.
     */
    public RedisClient(String host, int port) {
        ExecutorService connectors = Executors.newFixedThreadPool(1);
        ExecutorService workers    = Executors.newCachedThreadPool();
        ClientSocketChannelFactory factory = new NioClientSocketChannelFactory(connectors, workers);

        InetSocketAddress addr = new InetSocketAddress(host, port);

        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("remoteAddress", addr);

        setDefaultTimeout(60, TimeUnit.SECONDS);

        channels = new DefaultChannelGroup();
        timer    = new HashedWheelTimer();
    }
    
    /**
     * Create a new client that connects to the supplied host and port. Connection
     * attempts and non-blocking commands will {@link #setDefaultTimeout timeout}
     * after 60 seconds.
     *
     * @param host    Server hostname.
     * @param port    Server port.
     * @param listener Connection state listener
     */
    public RedisClient(String host, int port, IRedisConnectionStateListener listener) {
        this(host, port);
        this.connectionStateListener = listener;
    }

    /**
     * Set the default timeout for {@link RedisConnection connections} created by
     * this client. The timeout applies to connection attempts and non-blocking
     * commands.
     *
     * @param timeout   Default connection timeout.
     * @param unit      Unit of time for the timeout.
     */
    public void setDefaultTimeout(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit    = unit;
        bootstrap.setOption("connectTimeoutMillis", unit.toMillis(timeout));
    }

    /**
     * Open a new synchronous connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisConnection<String, String> connect() {
        return connect(new Utf8StringCodec());
    }

    /**
     * Open a new asynchronous connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisAsyncConnection<String, String> connectAsync() {
        return connectAsync(new Utf8StringCodec());
    }

    /**
     * Open a new pub/sub connection to the redis server that treats
     * keys and values as UTF-8 strings.
     *
     * @return A new connection.
     */
    public RedisPubSubConnection<String, String> connectPubSub() {
        return connectPubSub(new Utf8StringCodec());
    }

    /**
     * Open a new synchronous connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new connection.
     */
    public <K, V> RedisConnection<K, V> connect(RedisCodec<K, V> codec) {
        return new RedisConnection<K, V>(connectAsync(codec));
    }

    /**
     * Open a new asynchronous connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new connection.
     */
    public <K, V> RedisAsyncConnection<K, V> connectAsync(RedisCodec<K, V> codec) {
        BlockingQueue<Command<K, V, ?>> queue = new LinkedBlockingQueue<Command<K, V, ?>>();

        CommandHandler<K, V> handler = new CommandHandler<K, V>(queue);
        RedisAsyncConnection<K, V> connection = new RedisAsyncConnection<K, V>(queue, codec, timeout, unit);

        return connect(handler, connection);
    }

    /**
     * Open a new pub/sub connection to the redis server. Use the supplied
     * {@link RedisCodec codec} to encode/decode keys and values.
     *
     * @param codec Use this codec to encode/decode keys and values.
     *
     * @return A new pub/sub connection.
     */
    public <K, V> RedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
        BlockingQueue<Command<K, V, ?>> queue = new LinkedBlockingQueue<Command<K, V, ?>>();

        PubSubCommandHandler<K, V> handler = new PubSubCommandHandler<K, V>(queue, codec);
        RedisPubSubConnection<K, V> connection = new RedisPubSubConnection<K, V>(queue, codec, timeout, unit);

        return connect(handler, connection);
    }

    private <K, V, T extends RedisAsyncConnection<K, V>> T connect(CommandHandler<K, V> handler, T connection) {
        try {
            ConnectionWatchdog<K, V, T> watchdog = new ConnectionWatchdog<K, V, T>(bootstrap, channels, timer, this, connection);
            ChannelPipeline pipeline = Channels.pipeline(watchdog, handler, connection);
            Channel channel = bootstrap.getFactory().newChannel(pipeline);

            ChannelFuture future = channel.connect((SocketAddress) bootstrap.getOption("remoteAddress"));
            future.await();

            if (!future.isSuccess()) {
                throw future.getCause();
            }

            watchdog.setReconnect(true);

            return connection;
        } catch (Throwable e) {
            throw new RedisException("Unable to connect", e);
        }
    }
    
    /**
     * Gets client's connection state listener.
     * 
     * @return Connection state listener associated to this client.
     */
    public IRedisConnectionStateListener getConnectionStateListener() {
        return connectionStateListener;
    }
    
    /**
     * Sets client's connection state listener.
     * 
     * @param listener Connection state listener associated to this client.
     */
    public void setConnectionStateListener(IRedisConnectionStateListener listener) {
        connectionStateListener = listener;
    }    

    /**
     * Shutdown this client and close all open connections. The client should be
     * discarded after calling shutdown.
     */
    public void shutdown() {
        for (Channel c : channels) {
            ChannelPipeline pipeline = c.getPipeline();
            RedisAsyncConnection<?, ?> connection = pipeline.get(RedisAsyncConnection.class);
            connection.close();
        }
        ChannelGroupFuture future = channels.close();
        future.awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }
}

