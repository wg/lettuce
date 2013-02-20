// Copyright (C) 2011 - Will Glozer.  All rights reserved.

package com.lambdaworks.redis.pubsub;

import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.protocol.*;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;

import java.util.concurrent.BlockingQueue;

/**
 * A netty {@link ChannelHandler} responsible for writing redis pub/sub commands
 * and reading the response stream from the server.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 *
 * @author Will Glozer
 */
public class PubSubCommandHandler<K, V> extends CommandHandler<K, V> {
    private RedisCodec<K, V> codec;
    private PubSubOutput<K, V> output;

    /**
     * Initialize a new instance.
     *
     * @param queue Command queue.
     * @param codec Codec.
     */
    public PubSubCommandHandler(BlockingQueue<Command<K, V, ?>> queue, RedisCodec<K, V> codec) {
        super(queue);
        this.codec  = codec;
        this.output = new PubSubOutput<K, V>(codec);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ChannelBuffer buffer) throws InterruptedException {
        /**
         * Only process from the queue if we aren't in the middle of constructing output. If we do it will lead to
         * an Exception in {@link com.lambdaworks.redis.pubsub.PubSubOutput#set(java.nio.ByteBuffer)} when it attempts
         * to convert the next sequence of bytes into a valid {@link com.lambdaworks.redis.pubsub.PubSubOutput.Type}.
         */
        if (output.type() == null) {
            while (!queue.isEmpty()) {
                CommandOutput<K, V, ?> output = queue.peek().getOutput();
                if (!rsm.decode(buffer, output)) {
                    return;
                }
                queue.take().complete();
                if (output instanceof PubSubOutput) Channels.fireMessageReceived(ctx, output);
            }
        }

        while (rsm.decode(buffer, output)) {
            Channels.fireMessageReceived(ctx, output);
            output = new PubSubOutput<K, V>(codec);
        }
    }

}
