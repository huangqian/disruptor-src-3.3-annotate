/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import java.util.concurrent.locks.LockSupport;

import sun.misc.Unsafe;

import com.lmax.disruptor.util.Util;


/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p> * Note on {@link Sequencer#getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@link Sequencer#next()}, to determine the highest available sequence that can be read, then
 * {@link Sequencer#getHighestPublishedSequence(long, long)} should be used.</p>
 */
public final class MultiProducerSequencer extends AbstractSequencer
{
    private static final Unsafe UNSAFE = Util.getUnsafe();
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    /**
     * gatingSequenceCache是所有消费者都已经消费完毕的序号的最小值的缓存，在这个值之前的sequence都是可以让生产者使用的。
     * 这里缓存有一个好处，如果还剩余很多的空间可以供生产者使用，next函数拿到的值小于gatingSequenceCache，
     * 那么不需要重新从各个消费者sequence中读取最小值计算，直接分配空间，让生产者使用，以此提高性能
     */
    private final Sequence gatingSequenceCache = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);

    // availableBuffer tracks the state of each ringbuffer slot
    // see below for more details on the approach
    /**
     * <pre>
     * availableBuffer的存在是为了避免多个生产者publisher线程共享同一个sequence对象。
     *
     * availableBuffer的初始值都为-1。详见{@link MultiProducerSequencer#initialiseAvailableBuffer()}
     *
     * 在生产者调用publish方法的时候，将对应的sequence存储在availableBuffer中。
     * availableBuffer是一个int类型的数组，其存储方式如下
     *     index = sequence % bufferSize
     *     round = sequence / bufferSize
     *
     *     availableBuffer[index] = round
     *
     *
     *
     *
     * -- 首先，我们有一个限制，即cursor和 最小的gating sequence之差永远不会大于缓冲区的大小。
     *
     * -- 通过模运算作为缓冲区的索引
     *
     * -- 通过除法运算，得到sequence高位部分为的检查可用性的值，这个值能告诉我们已经在ringBuffer中运行了多少次。
     *
     * -- 因为我们不能在gating sequences不向前移动的情况下进行包装(即
     * 最小选通序列(minimum gating sequence)是我们在缓冲区中最后一个有效的可用位置)，当我们有新数据并成功地声明一个插槽时，我们可以简单地在上面写入。
     * </pre>
     */
    private final int[] availableBuffer;
    private final int indexMask;
    private final int indexShift;

    /**
     * Construct a Sequencer with the selected wait strategy and buffer size.
     *
     * @param bufferSize   the size of the buffer that this will sequence over.
     * @param waitStrategy for those waiting on sequences.
     */
    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy)
    {
        super(bufferSize, waitStrategy);
        availableBuffer = new int[bufferSize];
        indexMask = bufferSize - 1;
        indexShift = Util.log2(bufferSize);
        initialiseAvailableBuffer();
    }

    /**
     * @see Sequencer#hasAvailableCapacity(int)
     */
    @Override
    public boolean hasAvailableCapacity(final int requiredCapacity)
    {
        return hasAvailableCapacity(gatingSequences, requiredCapacity, cursor.get());
    }

    private boolean hasAvailableCapacity(Sequence[] gatingSequences, final int requiredCapacity, long cursorValue)
    {
        long wrapPoint = (cursorValue + requiredCapacity) - bufferSize;
        long cachedGatingSequence = gatingSequenceCache.get();

        if (wrapPoint > cachedGatingSequence || cachedGatingSequence > cursorValue)
        {
            long minSequence = Util.getMinimumSequence(gatingSequences, cursorValue);
            gatingSequenceCache.set(minSequence);

            if (wrapPoint > minSequence)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * @see Sequencer#claim(long)
     */
    @Override
    public void claim(long sequence)
    {
        cursor.set(sequence);
    }

    /**
     * @see Sequencer#next()
     */
    @Override
    public long next()
    {
        return next(1);
    }

    /**
     * @see Sequencer#next(int)
     */
    @Override
    public long next(int n)
    {
        if (n < 1 || n > bufferSize)
        {
            throw new IllegalArgumentException("n must be > 0 and < bufferSize");
        }

        long current;
        long next;

        do
        {
            current = cursor.get();
            next = current + n;

            long wrapPoint = next - bufferSize;
            long cachedGatingSequence = gatingSequenceCache.get();
            // wrapPoint < 0 表示buffer没有使用完，还有可用空间，因此不要处理
            //
            // wrapPoint >= 0 表示buffer已经被消费者使用完了至少一遍了，因此需要是否有位置给生产者使用
            // 如何确定是否有位置给生产使用？
            // 验证wrapPoint是否小于所有生产者都已经消费过的位置(min(gatingSequences))，我们把这个位置位置之前的认为可以使用的位置
            // 如果可以使用的位置大于等于wrapPoint，那么表示直接使用
            // 如果可以使用的位置小于wrapPoint，那么需要等消费者消费完毕后才能使用，否则会覆盖数据。

            if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
            {
                long gatingSequence = Util.getMinimumSequence(gatingSequences, current);

                if (wrapPoint > gatingSequence)
                {
                    LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
                    continue;
                }

                gatingSequenceCache.set(gatingSequence);
            }
            //通过cas设置cursor
            else if (cursor.compareAndSet(current, next))
            {
                break;
            }
        }
        while (true);

        return next;
    }

    /**
     * @see Sequencer#tryNext()
     */
    @Override
    public long tryNext() throws InsufficientCapacityException
    {
        return tryNext(1);
    }

    /**
     * @see Sequencer#tryNext(int)
     */
    @Override
    public long tryNext(int n) throws InsufficientCapacityException
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long current;
        long next;

        do
        {
            current = cursor.get();
            next = current + n;

            if (!hasAvailableCapacity(gatingSequences, n, current))
            {
                throw InsufficientCapacityException.INSTANCE;
            }
        }
        while (!cursor.compareAndSet(current, next));

        return next;
    }

    /**
     * @see Sequencer#remainingCapacity()
     */
    @Override
    public long remainingCapacity()
    {
        // 1. next是生产者下一个可以放入的位置
        // 2. cursor是最后一个commit的数据的位置
        // 3. consumed是所有消费都已经消费到的一个位置。在consumed之前的位置都是可以回收的空间
        //
        // +----------+------+--------+------+-------+
        // | consumed | .... | cursor | .... | next  |
        // +----------+------+--------+------+-------+
        //
        // next-consumed之间的数据表示没有消费到和正在提交的数据，这一部分是不能清理的
        // 可以空间 = bufferSize - (next - consumed);
        long consumed = Util.getMinimumSequence(gatingSequences, cursor.get());
        long produced = cursor.get();
        return getBufferSize() - (produced - consumed);
    }

    private void initialiseAvailableBuffer()
    {
        for (int i = availableBuffer.length - 1; i != 0; i--)
        {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    /**
     * @see Sequencer#publish(long)
     */
    @Override
    public void publish(final long sequence)
    {
        //可以获得的位置，available是可获得的意思，这里表示更新生产者可获得的sequence。
        setAvailable(sequence);
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * @see Sequencer#publish(long, long)
     */
    @Override
    public void publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            setAvailable(l);
        }
        waitStrategy.signalAllWhenBlocking();
    }

    /**
     * The below methods work on the availableBuffer flag.
     * <p>
     * The prime reason is to avoid a shared sequence object between publisher threads.
     * (Keeping single pointers tracking start and end would require coordination
     * between the threads).
     * <p>
     * --  Firstly we have the constraint that the delta between the cursor and minimum
     * gating sequence will never be larger than the buffer size (the code in
     * next/tryNext in the Sequence takes care of that).
     * -- Given that; take the sequence value and mask off the lower portion of the
     * sequence as the index into the buffer (indexMask). (aka modulo operator)
     * -- The upper portion of the sequence becomes the value to check for availability.
     * ie: it tells us how many times around the ring buffer we've been (aka division)
     * -- Because we can't wrap without the gating sequences moving forward (i.e. the
     * minimum gating sequence is effectively our last available position in the
     * buffer), when we have new data and successfully claimed a slot we can simply
     * write over the top.
     */
    /**
     * 下面的方法处理availableBuffer标志
     *
     * 主要原因是避免多个publisher（生产者提交）线程共享同一个sequence对象。（保持单指针跟踪开始和结束需要线程之间的协调）
     *
     * -- 首先，我们有一个限制，即cursor和 最小的gating sequence之差永远不会大于缓冲区的大小。
     *
     * -- 通过模运算作为缓冲区的索引
     *
     * -- 通过除法运算，得到sequence高位部分为的检查可用性的值，这个值能告诉我们已经在ringBuffer中运行了多少次。
     *
     * -- 因为我们不能在gating sequences不向前移动的情况下进行包装(即
     * 最小选通序列(minimum gating sequence)是我们在缓冲区中最后一个有效的可用位置)，当我们有新数据并成功地声明一个插槽时，我们可以简单地在上面写入。
     *
     */
    private void setAvailable(final long sequence)
    {
        //一个数字n  n = bufferSize* m + n % bufferSize;
        setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
    }

    private void setAvailableBufferValue(int index, int flag)
    {
        long bufferAddress = (index * SCALE) + BASE;
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    /**
     * @see Sequencer#isAvailable(long)
     */
    @Override
    public boolean isAvailable(long sequence)
    {
        // index = sequence % bufferSize
        int index = calculateIndex(sequence);
        // flag = sequence / bufferSize
        int flag = calculateAvailabilityFlag(sequence);
        long bufferAddress = (index * SCALE) + BASE;
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
        {
            if (!isAvailable(sequence))
            {
                return sequence - 1;
            }
        }

        return availableSequence;
    }

    private int calculateAvailabilityFlag(final long sequence)
    {
        //等价 sequence除以2的indexShift次幂
        // bufferSize = math.pow(2, indexShift)
        //因此 这里等价 sequence / bufferSize
        return (int) (sequence >>> indexShift);
    }

    private int calculateIndex(final long sequence)
    {
        //等价 sequence % (indexMask + 1)
        //因为bufferSize = indexMask + 1;
        // 这里其实就是 sequence % bufferSize
        return ((int) sequence) & indexMask;
    }
}
