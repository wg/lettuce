// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis.protocol;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.util.*;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A netty {@link ChannelHandler} responsible for monitoring the channel and
 * reconnecting when the connection is lost.
 *
 * @author Will Glozer
 */
public class ConnectionWatchdog extends SimpleChannelUpstreamHandler implements TimerTask {
    private ClientBootstrap bootstrap;
    private Channel channel;
    private ChannelGroup channels;
    private Timer timer;
    private boolean reconnect;
    private int attempts;

    /**
     * Create a new watchdog that adds to new connections to the supplied {@link ChannelGroup}
     * and establishes a new {@link Channel} when disconnected, while reconnect is true.
     *
     * @param bootstrap Configuration for new channels.
     * @param channels  ChannelGroup to add new channels to.
     * @param timer     Timer used for delayed reconnect.
     */
    public ConnectionWatchdog(ClientBootstrap bootstrap, ChannelGroup channels, Timer timer) {
        this.bootstrap = bootstrap;
        this.channels  = channels;
        this.timer     = timer;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channel = ctx.getChannel();
        channels.add(channel);
        attempts = 0;
        ctx.sendUpstream(e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (reconnect) {
            if (attempts < 8) attempts++;
            int timeout = 2 << attempts;
            timer.newTimeout(this, timeout, TimeUnit.MILLISECONDS);
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        ctx.getChannel().close();
    }

    /**
     * Reconnect to the remote address that the closed channel was connected to.
     * This creates a new {@link ChannelPipeline} with the same handler instances
     * contained in the old channel's pipeline.
     *
     * @param timeout Timer task handle.
     *
     * @throws Exception when reconnection fails.
     */
    @Override
    public void run(Timeout timeout) throws Exception {
        ChannelPipeline old = channel.getPipeline();
        List<ChannelHandler> handlers = new ArrayList<ChannelHandler>();
        for (String name : old.getNames()) {
            handlers.add(old.get(name));
        }
        ChannelPipeline pipeline = Channels.pipeline(handlers.toArray(new ChannelHandler[handlers.size()]));

        Channel c = bootstrap.getFactory().newChannel(pipeline);
        c.getConfig().setOptions(bootstrap.getOptions());
        c.connect((SocketAddress) bootstrap.getOption("remoteAddress"));
    }
}
