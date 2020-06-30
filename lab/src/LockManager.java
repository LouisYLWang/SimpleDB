package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LockManager {
    public HashMap<PageId, LocksOnPage> locks;
    public HashMap<TransactionId, HashSet<PageId>> transactions;
    public HashMap<TransactionId, HashSet<TransactionId>> waitForGraph;

    class LocksOnPage{
        private TransactionId exclusiveLock;
        private HashSet<TransactionId> sharedLocks;

        LocksOnPage(){
            this.exclusiveLock = null;
            this.sharedLocks = new HashSet<>();
        }

        public Boolean hasExclusiveLock(){
            return this.exclusiveLock != null;
        }

        public Boolean hasSharedLocks(){
            return this.sharedLocks.isEmpty();
        }

        public TransactionId getExclusiveLock(){
            return this.exclusiveLock;
        }

        public HashSet<TransactionId> getSharedLocks(){
            return this.sharedLocks;
        }

        public void addExclusiveLock(TransactionId tid){
            this.exclusiveLock = tid;
        }

        public void addSharedLocks(TransactionId tid){
            this.sharedLocks.add(tid);
        }

        public void removeExclusiveLock(TransactionId tid){
            if (exclusiveLock == tid){
                this.exclusiveLock = null;
            }
        }

        public void removeSharedLocks(TransactionId tid){
            if (sharedLocks.contains(tid)){
                sharedLocks.remove(tid);
            }
        }
    }

    public LockManager() {
        locks = new HashMap<>();
        transactions = new HashMap<>();
        waitForGraph = new HashMap<>();
    }

    public synchronized void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, InterruptedException {
        this.locks.putIfAbsent(pid, new LocksOnPage());
        this.transactions.putIfAbsent(tid, new HashSet<>());
        LocksOnPage curLocksSetOnPage = this.locks.get(pid);

        while (this.isLocked(pid)){
            if (!curLocksSetOnPage.hasExclusiveLock()){
                if (perm.equals(Permissions.READ_WRITE)){
                    if (curLocksSetOnPage.getSharedLocks().contains(tid)){
                        if (upgrade(tid, pid)) {
                            return;
                        }
                    }
                    this.waitForGraph.putIfAbsent(tid, new HashSet<>());
                    for(TransactionId waitForTid:curLocksSetOnPage.getSharedLocks()){
                        if (waitForTid != tid){
                            this.waitForGraph.get(tid).add(waitForTid);
                        }
                    }
                    if (detectDeadlock()){
                        for(TransactionId waitForTid:curLocksSetOnPage.getSharedLocks()){
                            if (waitForTid != tid){
                                this.waitForGraph.get(tid).remove(waitForTid);
                            }
                        }
                        notifyAll();
                        throw new TransactionAbortedException();
                    }
                } else if (perm.equals(Permissions.READ_ONLY)){
                    if (isLocked(tid, pid)){
                        return;
                    }
                    curLocksSetOnPage.addSharedLocks(tid);
                    this.transactions.putIfAbsent(tid, new HashSet<>());
                    transactions.get(tid).add(pid);
                    return;
                }
            } else {
                if (curLocksSetOnPage.getExclusiveLock() == tid){
                    return;
                } else {
                    this.waitForGraph.putIfAbsent(tid, new HashSet<>());
                    this.waitForGraph.get(tid).add(curLocksSetOnPage.getExclusiveLock());
                    if (detectDeadlock()){
                        this.waitForGraph.get(tid).remove(curLocksSetOnPage.getExclusiveLock());
                        notifyAll();
                        throw new TransactionAbortedException();
                    }
                }
            }
            wait();
        }

        if (perm.equals(Permissions.READ_WRITE)) {
            curLocksSetOnPage.addExclusiveLock(tid);
        }
        if (perm.equals(Permissions.READ_ONLY)) {
            curLocksSetOnPage.addSharedLocks(tid);
        }
        this.transactions.putIfAbsent(tid, new HashSet<>());
        transactions.get(tid).add(pid);
    }

    public synchronized Boolean upgrade(TransactionId tid, PageId pid) {
        LocksOnPage locksOnCurPage = locks.get(pid);
        if (locksOnCurPage.getSharedLocks().size() == 1) {
            locksOnCurPage.removeSharedLocks(tid);
            locksOnCurPage.addExclusiveLock(tid);
            return true;
        }
        return false;
    }

    public synchronized Boolean release(TransactionId tid, PageId pid) {
        if (locks.containsKey(pid)){
            LocksOnPage locksOnCurPage = locks.get(pid);
            if (locksOnCurPage.getExclusiveLock() == tid){
                locksOnCurPage.removeExclusiveLock(tid);
            }
            if (locksOnCurPage.getSharedLocks().contains(tid)){
                locksOnCurPage.removeSharedLocks(tid);
            }
            locks.remove(pid);
            notifyAll();
            return true;
        }
        return false;
    }

    public synchronized void releaseAll(TransactionId tid) {
        if (transactions.containsKey(tid)){
            Iterator<PageId> it = transactions.get(tid).iterator();
            while (it.hasNext()){
                PageId pid = it.next();
                this.release(tid, pid);
                it.remove();
            }
        }
        waitForGraph.remove(tid);
    }

    public synchronized Boolean isLocked(PageId pid) {
        if (locks.containsKey(pid)){
            LocksOnPage locksOnCurPage = locks.get(pid);
            if (locksOnCurPage.getExclusiveLock() != null){
                return true;
            }
            return locksOnCurPage.getSharedLocks().size() > 0;
        }
        return false;
    }

    public synchronized Boolean isLocked(TransactionId tid, PageId pid) {
        if (locks.containsKey(pid)){
            LocksOnPage locksOnCurPage = locks.get(pid);
            if (locksOnCurPage.getExclusiveLock() == tid){
                return true;
            }
            return locksOnCurPage.getSharedLocks().contains(tid);
        }
        return false;
    }

    public synchronized boolean detectDeadlock() throws TransactionAbortedException {
        HashMap<TransactionId, Integer> indegree = new HashMap<>();
        Deque<TransactionId> queue = new LinkedList<>();

        for (TransactionId tid: this.transactions.keySet()){
            indegree.putIfAbsent(tid, 0);
        }

        for (TransactionId waiterTid: this.transactions.keySet()){
            if (this.waitForGraph.containsKey(waiterTid)){
                for (TransactionId waiteeTid: this.waitForGraph.get(waiterTid)){
                    indegree.replace(waiteeTid, indegree.get(waiteeTid)+1);
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
                    indegree.replace(waiteeTid, indegree.get(waiteeTid)-1);
                }
                if(indegree.get(waiteeTid) == 0) {
                    queue.offer(waiteeTid);
                }
            }
        }
        return count != this.transactions.size();
    }
}




