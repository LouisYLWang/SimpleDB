package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager keeps track of which locks each transaction holds and checks to see if a lock should be granted to a
 * transaction when it is requested.
 */
public class LockManager {
    public ConcurrentHashMap<PageId, LocksOnPage> locks;
    public ConcurrentHashMap<TransactionId, HashSet<PageId>> transactions;
    public ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitForGraph;

    private class LocksOnPage {
        private final LockManager lockManager;
        final PageId pageId;
        Permissions permissions;
        final HashSet<TransactionId> lockedBy;
        final HashSet<TransactionId> waitedBy;

        LocksOnPage(LockManager lockManager, PageId pid, TransactionId tid, Permissions perm) {
            this.lockManager = lockManager;
            this.pageId = pid;
            this.permissions = perm;
            HashSet<TransactionId> s = new HashSet<>();
            s.add(tid);
            this.lockedBy = s;
            this.waitedBy = new HashSet<>();
        }

        private boolean canAcquire(TransactionId tid, Permissions perm) {
            if (this.permissions == Permissions.READ_ONLY) {
                if (perm == Permissions.READ_ONLY) {
                    return true;
                } else if (perm == Permissions.READ_WRITE) {
                    return this.lockedBy.size() == 1 && this.lockedBy.contains(tid);
                }
            } else if (this.permissions == Permissions.READ_WRITE) {
                return this.lockedBy.contains(tid);
            }
            return true;
        }

        synchronized void lock(TransactionId tid, Permissions perm) throws InterruptedException, TransactionAbortedException {
            while (!canAcquire(tid, perm)) {
                this.lockManager.addToWaitForGraph(tid, this);
                this.waitedBy.add(tid);
                if (this.lockManager.detectDeadLock()) {
                    this.lockedBy.remove(tid);
                    this.waitedBy.remove(tid);
                    this.lockManager.removeFromWaitForGraph(tid, this.waitedBy);
                    notifyAll();
                    throw new TransactionAbortedException();
                }
                wait();
            }

            this.permissions = perm;
            this.lockedBy.add(tid);
            this.waitedBy.remove(tid);
            for (TransactionId waiter : this.waitedBy) {
                this.lockManager.addToWaitForGraph(waiter, this);
            }
            this.lockManager.registerTransaction(tid, this.pageId);
        }

        synchronized void unlock(TransactionId tid) {
            this.lockedBy.remove(tid);
            if (this.lockedBy.isEmpty()) {
                this.permissions = null;
            }
            this.lockManager.removeFromWaitForGraph(tid, this.waitedBy);
            this.lockManager.removeTransaction(tid, this.pageId);
            notifyAll();
        }
    }


    public LockManager() {
        this.locks = new ConcurrentHashMap<>();
        this.transactions = new ConcurrentHashMap<>();
        this.waitForGraph = new ConcurrentHashMap<>();
    }

    private synchronized void registerTransaction(TransactionId tid, PageId pid) {
        HashSet<PageId> pageSet = this.transactions.get(tid);
        if (pageSet == null) {
            pageSet = new HashSet<>();
            this.transactions.put(tid, pageSet);
        }
        pageSet.add(pid);
    }

    private synchronized void removeTransaction(TransactionId tid, PageId pid) {
        HashSet<PageId> pageSet = this.transactions.get(tid);
        if (pageSet == null) {
            return;
        }
        pageSet.remove(pid);
        if (pageSet.isEmpty()) {
            this.transactions.remove(tid);
        }
    }

    public void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        LocksOnPage locksOnPage = this.locks.get(pid);
        if (locksOnPage == null) {
            locksOnPage = new LocksOnPage(this, pid, tid, perm);
            LocksOnPage previous = this.locks.putIfAbsent(pid, locksOnPage);
            if (previous != null) {
                locksOnPage = previous;
            }
        }

        try {
            locksOnPage.lock(tid, perm);
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }
    }

    public void release(TransactionId tid, PageId pid) {
        LocksOnPage locksOnPage = this.locks.get(pid);
        if (locksOnPage != null) {
            locksOnPage.unlock(tid);
        }
    }

    private synchronized void addToWaitForGraph(TransactionId waiter, LocksOnPage locksOnPage) {
        HashSet<TransactionId> waitees = this.waitForGraph.computeIfAbsent(waiter, k -> new HashSet<>());
        for (TransactionId t:locksOnPage.lockedBy) {
            if (!t.equals(waiter)) {
                waitees.add(t);
            }
        }
    }

    private synchronized void removeFromWaitForGraph(TransactionId tid, Iterable<TransactionId> waiters) {
        this.waitForGraph.remove(tid);
        for (TransactionId waiter:waiters) {
            HashSet<TransactionId> waiting = this.waitForGraph.get(waiter);
            if (waiting != null) {
                waiting.remove(tid);
            }
        }
    }


    public synchronized boolean detectDeadLock() {
        HashMap<TransactionId, Integer> indegree = new HashMap<>();
        Deque<TransactionId> queue = new LinkedList<>();

        for (TransactionId tid: this.transactions.keySet()){
            indegree.putIfAbsent(tid, 0);
        }

        for (TransactionId waiterTid: this.transactions.keySet()){
            if (this.waitForGraph.containsKey(waiterTid)){
                for (TransactionId waiteeTid: this.waitForGraph.get(waiterTid)){
                    indegree.replace(waiteeTid, indegree.get(waiteeTid) + 1);
                }
            }
        }

        for (TransactionId tid: this.transactions.keySet()){
            if(indegree.get(tid) == 0){
                queue.offer(tid);
            }
        }

        int count = 0;
        while (!queue.isEmpty()){
            TransactionId waiterTid = queue.poll();
            count += 1;
            if (!this.waitForGraph.containsKey(waiterTid) ){
                continue;
            }

            for (TransactionId waiteeTid: this.waitForGraph.get(waiterTid)){
                if(indegree.get(waiteeTid) != 0){
                    indegree.replace(waiteeTid, indegree.get(waiteeTid) - 1);
                }
                if(indegree.get(waiteeTid) == 0) {
                    queue.offer(waiteeTid);
                }
            }
        }
        return count != this.transactions.size();
    }



    public synchronized boolean isLocked(TransactionId tid, PageId pid) {
        HashSet<PageId> pageSet = this.transactions.get(tid);
        return pageSet != null && pageSet.contains(pid);
    }

    public void releaseAll(TransactionId tid) {
        Iterator<PageId> it;
        synchronized (this) {
            HashSet<PageId> s = this.transactions.remove(tid);
            if (s == null) {
                return;
            }
            it = s.iterator();
        }

        while (it.hasNext()) {
            PageId pid = it.next();
            it.remove();
            release(tid, pid);
        }
    }
}