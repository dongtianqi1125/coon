package io.coon.api;

import java.util.concurrent.TimeUnit;

/**
 * The Micro Service Lock.
 *
 * @author lry
 **/
public interface Mlock {

    /**
     * 获取锁，如果锁被占用，将禁用当前线程，并且在获得锁之前，该线程将一直处于阻塞状态
     */
    void lock();

    /**
     * 若果锁可用，则获取锁，并立即返回true。如果锁不可用，则此方法将立即返回false
     *
     * @return
     */
    boolean tryLock();

    /**
     * 如果在给定时间内锁可用，则获取锁，并立即返回true。如果在给定时间内锁一直不可用，则此方法将立即返回false
     *
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException;

    /**
     * 获取锁，如果锁被占用，则一直等待直到线程被中断或者获取到锁
     *
     * @throws InterruptedException
     */
    void lockInterruptibly() throws InterruptedException;

    /**
     * 释放当前持有的锁
     */
    void unlock();
    
}
