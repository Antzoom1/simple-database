package simpledb;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;




public class LockManager {
    private ConcurrentHashMap<PageId, PageLocks> lockMap;
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> transactionMap;
    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitingMap;


    private class PageLocks {
        // create a new page lock object
        private final PageId pid;
        private final HashSet<TransactionId> lockedBy;
        private Permissions permission;
        private final LockManager lockManager;
        private final HashSet<TransactionId> waitedBy;

        PageLocks(PageId pid, TransactionId tid, Permissions permission, LockManager lockManager) {
            this.pid = pid;
            this.permission = permission;
            this.lockedBy = new HashSet<>();
            this.waitedBy = new HashSet<>();
            this.lockedBy.add(tid);
            this.lockManager = lockManager;
        }

        // register a transaction with the page
        private boolean canAcquire(TransactionId tid, Permissions p) {
            // if the page is not locked, then it can be acquired
            if (permission == Permissions.READ_ONLY && p == Permissions.READ_WRITE) {
                    return (lockedBy.size() == 1) && (lockedBy.contains(tid));
            // if the page is locked by the same transaction, then it can be acquired
            } else if (permission == Permissions.READ_WRITE) {
                return lockedBy.contains(tid);
            }
            // if the page is locked by another transaction, then it cannot be acquired
            return true;
        }

        synchronized void lock(TransactionId tid, Permissions p) throws InterruptedException, TransactionAbortedException {
            // if the page is already locked, then wait until it is unlocked
            while (!(canAcquire(tid, p))) {
                lockManager.waitMapAdd(tid, this);
                waitedBy.add(tid);
                if (lockManager.checkDeadLock()) {
                    waitedBy.remove(tid);
                    lockedBy.remove(tid);
                    lockManager.waitMapRemove(tid, waitedBy);
                    notifyAll();
                    throw new TransactionAbortedException();
                }
                wait();
            }
            // if the page is not locked, then acquire the lock
            permission = p;
            lockedBy.add(tid);
            waitedBy.remove(tid);
            for (TransactionId transaction : waitedBy) {
                this.lockManager.waitMapAdd(transaction, this);
            }
            lockManager.addTransaction(tid, pid);
        }

        // unlocks the page
        synchronized void unlock(TransactionId tid) {
            lockedBy.remove(tid);
            if (lockedBy.isEmpty()) {
                permission = null;
            }
            // remove the transaction from the lock map
            lockManager.waitMapRemove(tid, waitedBy);
            lockManager.removeTransaction(tid, pid);
            notifyAll();
        }

        // returns the transactions that are currently holding the lock
        public HashSet<TransactionId> getLockedBy() {
            return this.lockedBy;
        }
    }

    public LockManager() {
        // initialize the lock map
        this.lockMap = new ConcurrentHashMap<>();
        // initialize the waiting map
        this.waitingMap = new ConcurrentHashMap<>();
        // initialize the transaction map
        this.transactionMap = new ConcurrentHashMap<>();
    }

    // register a transaction with a page
    public void acquire(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        PageLocks pageLock = lockMap.get(pid);
        if (pageLock == null) {
            pageLock = new PageLocks(pid, tid, perm, this);
            PageLocks previous = lockMap.putIfAbsent(pid, pageLock);
            if (!(previous == null)) {
                pageLock = previous;
            }
        }
        try {
            // acquire the lock
            pageLock.lock(tid, perm);
        } catch (InterruptedException e) {
            throw new TransactionAbortedException();
        }
    }

    // remove a transaction from the lock map
    public void release(TransactionId tid, PageId pid) {
        PageLocks pageLock = lockMap.get(pid);
        if (pageLock != null) {
            pageLock.unlock(tid);
        }
    }

    // checks if a transaction has a lock on a page
    public synchronized boolean isLocked(TransactionId tid, PageId pid) {
        return transactionMap.get(tid).contains(pid);
    }

    // checks if a transaction has a lock on a page
    public void releaseLocks(TransactionId tid) {
        Iterator<PageId> iterator;
        synchronized (this) {
            HashSet<PageId> pId = transactionMap.remove(tid);
            if (pId == null) {
                return;
            }
            iterator = pId.iterator();
        }
        // release all the locks
        while (iterator.hasNext()) {
            PageId pid = iterator.next();
            iterator.remove();
            release(tid, pid);
        }
    }

    // register a transaction with a page
    public synchronized void waitMapAdd(TransactionId tid, PageLocks pl) {
        HashSet<TransactionId> waitingLocks = waitingMap.computeIfAbsent(tid, k -> new HashSet<>());
        for (TransactionId tid2 : pl.getLockedBy()) {
            if (!(tid.equals(tid2))) {
                waitingLocks.add(tid2);
            }
        }
    }

    public synchronized void waitMapRemove(TransactionId tid, Iterable<TransactionId> waitList) {
        waitingMap.remove(tid);
        for (TransactionId tid2 : waitList) {
            HashSet<TransactionId> inQueue = waitingMap.get(tid2);
            if (inQueue != null) {
                inQueue.remove(tid);
            }
        }
    }
    

    // adds a transaction to the lock map
    private synchronized void addTransaction(TransactionId tid, PageId pid) {
        HashSet<PageId> setPage = transactionMap.computeIfAbsent(tid, k->new HashSet<>());
        setPage.add(pid);
    }

    // returns the lock map
    public ConcurrentHashMap<TransactionId, HashSet<PageId>> getTransactionMap() {
        return this.transactionMap;
    }

    // removes a transaction from the lock map
    private synchronized void removeTransaction(TransactionId tid, PageId pid) {
        HashSet<PageId> setPage = transactionMap.get(tid);
        if (setPage != null) {
            setPage.remove(pid);
            if (setPage.isEmpty()) {
                transactionMap.remove(tid);
            }
        }
    }
    // checks if a deadlock has occurred
    public synchronized boolean checkDeadLock() {
        // checks if there is a cycle in the waiting graph
        HashMap<TransactionId, Integer> childDegree = new HashMap<>();
        Deque<TransactionId> deque = new LinkedList<>();

        for (TransactionId tid: transactionMap.keySet()){
            childDegree.putIfAbsent(tid, 0);
        }
        // calculate the indegree of each transaction
        for (TransactionId tid: transactionMap.keySet()){
            if (waitingMap.containsKey(tid)){
                // add the indegree of the children
                for (TransactionId tid2: waitingMap.get(tid)){
                    childDegree.replace(tid2, childDegree.get(tid2) + 1);
                }
            }
        }
        // add all the transactions with indegree 0 to the queue
        for (TransactionId tid: transactionMap.keySet()){
            // if the transaction is not waiting for any other transaction, then add it to the queue
            if(childDegree.get(tid) == 0){
                deque.offer(tid);
            }
        }

        // remove all the transactions from the queue and add their children to the queue
        int count = 0;
        while (!(deque.isEmpty())){
            TransactionId tId = deque.poll();
            count += 1;
            // add the children of the transaction to the queue
            if (waitingMap.containsKey(tId) ) {
                for (TransactionId tid2 : waitingMap.get(tId)) {
                    if (!(childDegree.get(tid2) == 0)) {
                        // decrease the indegree of the child
                        childDegree.replace(tid2, childDegree.get(tid2) - 1);
                    } else {
                        // add the child to the queue
                        deque.offer(tid2);
                    }
                }
            }
        }
        // if the number of transactions in the queue is not equal to the number of transactions in the lock map, then a cycle exists
        return count != transactionMap.size();
    }
}