/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.delayed.bucket;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.delayed.proto.DelayedIndex;
import org.apache.pulsar.broker.delayed.proto.SnapshotMetadata;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegment;
import org.apache.pulsar.broker.delayed.proto.SnapshotSegmentMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.TripleLongPriorityQueue;
import org.roaringbitmap.RoaringBitmap;

@Slf4j
class MutableBucket extends Bucket implements AutoCloseable {

    private final TripleLongPriorityQueue priorityQueue;

    MutableBucket(String dispatcherName, ManagedCursor cursor, FutureUtil.Sequencer<Void> sequencer,
                  BucketSnapshotStorage bucketSnapshotStorage) {
        super(dispatcherName, cursor, sequencer, bucketSnapshotStorage, -1L, -1L);
        this.priorityQueue = new TripleLongPriorityQueue();
    }

    /**
     * 封存并持久化bucket
     * 
     * @param timeStepPerBucketSnapshotSegment 每个segment的延时时间最长跨度，默认300秒
     * @param maxIndexesPerBucketSnapshotSegment 每个segment数据量限制，-1代表无限制
     * @param sharedQueue  共享队列sharedBucketPriorityQueue
     * @return left: 新建的bucket  right: 新bucket的第一个segment的最后一条数据
     */
    Pair<ImmutableBucket, DelayedIndex> sealBucketAndAsyncPersistent(
            long timeStepPerBucketSnapshotSegment,
            int maxIndexesPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue) {
        return createImmutableBucketAndAsyncPersistent(timeStepPerBucketSnapshotSegment,
                maxIndexesPerBucketSnapshotSegment, sharedQueue,
                TripleLongPriorityDelayedIndexQueue.wrap(priorityQueue), startLedgerId, endLedgerId);
    }

    /**
     * 创建并持久化ImmutableBucket
     * <p/>
     * 额外的影响：
     * 1、第一个segment的数据会被加入到sharedQueue
     * 2、构造ImmutableBucket，并持久化
     * 
     * @param timeStepPerBucketSnapshotSegment 每个segment的延时时间最长跨度，默认300秒
     * @param maxIndexesPerBucketSnapshotSegment 每个segment数据量限制，-1代表无限制
     * @param sharedQueue  共享队列sharedBucketPriorityQueue
     * @param delayedIndexQueue 待写入到ImmutableBucket的延迟数据
     * @param startLedgerId MutableBucket的起始ledgerId
     * @param endLedgerId MutableBucket的结束ledgerId
     * @return left: 新建的bucket  right: 新bucket的第一个segment的最后一条数据
     */
    Pair<ImmutableBucket, DelayedIndex> createImmutableBucketAndAsyncPersistent(
            final long timeStepPerBucketSnapshotSegment, final int maxIndexesPerBucketSnapshotSegment,
            TripleLongPriorityQueue sharedQueue, DelayedIndexQueue delayedIndexQueue, final long startLedgerId,
            final long endLedgerId) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating bucket snapshot, startLedgerId: {}, endLedgerId: {}", dispatcherName, startLedgerId, endLedgerId);
        }

        if (delayedIndexQueue.isEmpty()) {
            return null;
        }
        // 处理的消息数量
        long numMessages = 0;

        // ImmutableBucket的所有segment
        List<SnapshotSegment> bucketSnapshotSegments = new ArrayList<>();
        // ImmutableBucket的所有segment的元数据
        List<SnapshotSegmentMetadata> segmentMetadataList = new ArrayList<>();

        // 整个bucket的bitmap ，key: ledgerId  value: bitmap(entryId)
        Map<Long, RoaringBitmap> immutableBucketBitMap = new HashMap<>();

        // 当前segment的bitmap，key: ledgerId  value: bitmap(entryId)
        Map<Long, RoaringBitmap> bitMap = new HashMap<>();
        // 当前循环的segment
        SnapshotSegment snapshotSegment = new SnapshotSegment();
        
        // segment元信息的builder
        SnapshotSegmentMetadata.Builder segmentMetadataBuilder = SnapshotSegmentMetadata.newBuilder();

        // 所有segment的多个延时时间戳
        List<Long> firstScheduleTimestamps = new ArrayList<>();
        // 当前segment可以容纳的最大延时时间戳
        long currentTimestampUpperLimit = 0;
        // 当前segment的首个延时时间戳
        long currentFirstTimestamp = 0L;
        
        while (!delayedIndexQueue.isEmpty()) {
            final long timestamp = delayedIndexQueue.peekTimestamp();
            if (currentTimestampUpperLimit == 0) {
                currentFirstTimestamp = timestamp;
                firstScheduleTimestamps.add(currentFirstTimestamp);
                currentTimestampUpperLimit = timestamp + timeStepPerBucketSnapshotSegment - 1;
            }

            // 在snapshotSegment中，新建一条空的数据
            DelayedIndex delayedIndex = snapshotSegment.addIndexe();
            // 用队列的第一条数据，给delayedIndex赋值
            delayedIndexQueue.popToObject(delayedIndex);

            final long ledgerId = delayedIndex.getLedgerId();
            final long entryId = delayedIndex.getEntryId();

            // 从当前bucket的位图中删除entry
            removeIndexBit(ledgerId, entryId);

            checkArgument(ledgerId >= startLedgerId && ledgerId <= endLedgerId);

            // segmentMetadataList在第一个segment填满时会被add SnapshotSegmentMetadata
            // 因此segmentMetadataList非空了，就是第二个segment了
            // Move first segment of bucket snapshot to sharedBucketPriorityQueue
            if (segmentMetadataList.isEmpty()) {
                sharedQueue.add(timestamp, ledgerId, entryId);
            }

            bitMap.computeIfAbsent(ledgerId, k -> new RoaringBitmap()).add(entryId, entryId + 1);

            numMessages++;

            /*
             * 是否停止写入当前segment
             * 1、delayedIndexQueue无剩余数据
             * 2、或下一条数据的延时时间已经超过了当前segment的最长跨度
             * 3、或segment数据量达到限制
             */
            if (delayedIndexQueue.isEmpty() || 
                    delayedIndexQueue.peekTimestamp() > currentTimestampUpperLimit ||
                    (maxIndexesPerBucketSnapshotSegment != -1 && snapshotSegment.getIndexesCount() >= maxIndexesPerBucketSnapshotSegment)) {
                // segment元信息中的最大时间戳
                segmentMetadataBuilder.setMaxScheduleTimestamp(timestamp);
                // segment元信息中的最小时间戳
                segmentMetadataBuilder.setMinScheduleTimestamp(currentFirstTimestamp);
                // 下一个segment重新开始处理，初始化「当前segment可以容纳的最大延时时间戳」
                currentTimestampUpperLimit = 0;
                
                // key: ledgerId
                Iterator<Map.Entry<Long, RoaringBitmap>> iterator = bitMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    final var entry = iterator.next();
                    // ledgerId
                    final var lId = entry.getKey();
                    // RoaringBitmap(entryId)
                    final var bm = entry.getValue();
                    bm.runOptimize();
                    // 根据RoaringBitmap序列化后的字节数，创建ByteBuffer
                    ByteBuffer byteBuffer = ByteBuffer.allocate(bm.serializedSizeInBytes());
                    // 将RoaringBitmap序列化到byteBuffer
                    bm.serialize(byteBuffer);
                    // 翻转，准备写出
                    byteBuffer.flip();
                    // 在metadata中put，ledgerId => bitmap(entryId)
                    segmentMetadataBuilder.putDelayedIndexBitMap(lId, UnsafeByteOperations.unsafeWrap(byteBuffer));
                    immutableBucketBitMap.compute(lId, (__, bm0) -> {
                        if (bm0 == null) {
                            return bm;
                        }
                        // bitmap之前做或运算，相当于两个集合做并集
                        bm0.or(bm);
                        return bm0;
                    });
                    iterator.remove();
                }

                // 构建并添加元数据
                segmentMetadataList.add(segmentMetadataBuilder.build());
                segmentMetadataBuilder.clear();
                
                // 添加segment
                bucketSnapshotSegments.add(snapshotSegment);
                // 创建新的segment
                snapshotSegment = new SnapshotSegment();
            }
        }

        // optimize bm
        immutableBucketBitMap.values().forEach(RoaringBitmap::runOptimize);
        this.delayedIndexBitMap.values().forEach(RoaringBitmap::runOptimize);

        // bucket的元数据
        SnapshotMetadata bucketSnapshotMetadata = SnapshotMetadata.newBuilder()
                .addAllMetadataList(segmentMetadataList)
                .build();

        // 看起来entryId是从1开始递增的，因此最后一个entryId可以用size()替代
        final int lastSegmentEntryId = segmentMetadataList.size();

        ImmutableBucket bucket = new ImmutableBucket(dispatcherName, cursor, sequencer, bucketSnapshotStorage, startLedgerId, endLedgerId);
        bucket.setCurrentSegmentEntryId(1);
        bucket.setNumberBucketDelayedMessages(numMessages);
        bucket.setLastSegmentEntryId(lastSegmentEntryId);
        bucket.setFirstScheduleTimestamps(firstScheduleTimestamps);
        bucket.setDelayedIndexBitMap(immutableBucketBitMap);

        // Skip first segment, because it has already been loaded
        List<SnapshotSegment> snapshotSegments = bucketSnapshotSegments.subList(1, bucketSnapshotSegments.size());
        bucket.setSnapshotSegments(snapshotSegments);

        // Add the first snapshot segment last message to snapshotSegmentLastMessageTable
        checkArgument(!bucketSnapshotSegments.isEmpty());
        SnapshotSegment firstSnapshotSegment = bucketSnapshotSegments.get(0);
        DelayedIndex lastDelayedIndex = firstSnapshotSegment.getIndexeAt(firstSnapshotSegment.getIndexesCount() - 1);
        Pair<ImmutableBucket, DelayedIndex> result = Pair.of(bucket, lastDelayedIndex);

        CompletableFuture<Long> future = asyncSaveBucketSnapshot(bucket, bucketSnapshotMetadata, bucketSnapshotSegments);
        bucket.setSnapshotCreateFuture(future);

        return result;
    }

    /**
     * 将截止时间以前的数据迁移到入参的队列中
     */
    void moveScheduledMessageToSharedQueue(long cutoffTime, TripleLongPriorityQueue sharedBucketPriorityQueue) {
        while (!priorityQueue.isEmpty()) {
            long timestamp = priorityQueue.peekN1();
            if (timestamp > cutoffTime) {
                break;
            }

            long ledgerId = priorityQueue.peekN2();
            long entryId = priorityQueue.peekN3();
            sharedBucketPriorityQueue.add(timestamp, ledgerId, entryId);

            priorityQueue.pop();
        }
    }

    void resetLastMutableBucketRange() {
        this.startLedgerId = -1L;
        this.endLedgerId = -1L;
    }

    void clear() {
        this.resetLastMutableBucketRange();
        this.delayedIndexBitMap.clear();
        this.priorityQueue.clear();
    }

    public void close() {
        priorityQueue.close();
    }

    long getBufferMemoryUsage() {
        return priorityQueue.bytesCapacity();
    }

    boolean isEmpty() {
        return priorityQueue.isEmpty();
    }

    long nextDeliveryTime() {
        return priorityQueue.peekN1();
    }

    long size() {
        return priorityQueue.size();
    }

    void addMessage(long ledgerId, long entryId, long deliverAt) {
        priorityQueue.add(deliverAt, ledgerId, entryId);
        if (startLedgerId == -1L) {
            this.startLedgerId = ledgerId;
        }
        this.endLedgerId = ledgerId;
        putIndexBit(ledgerId, entryId);
    }
}
