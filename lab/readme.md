Yiliang Wang 

CSE 444 

LAB 3 WRITE-UP

### Demonstrate your understanding

The main goal of Lab 3 is to implement the Transactions functionality of the simple DB using NO STEAL and FORCE  buffer management policy in the granularity of page level. NO STEAL means the eviction of dirty pages only happened after a transaction is committed. FORCE means the dirty pages are forced to flush to disk after a transaction is committed.

The whole picture of the lab is:

- LockManager:
  - LockOnPage: class to organize locks on specific page
  - locks hashmap: locks store - link page to LockOnPage, use for find lock on specific page
  - transaction hashmap: transaction store - link transaction to page it lock
  - waitForGraph: use for deadlock detection
  - acquire / upgrade: acquire read/write lock and upgrade read lock 
  - detectDeadlock(): detect deadlock
  - release(): release all lock of a transaction
- BufferPool:
  - getPage(): add aquire lock before getting file from disk
  - transactionComplete: NO STEAL and FORCE part of policy
  - adjustment of evictPage: NO STEAL part of policy
  - *releasePage()
  - *holdsLock() (does not use this function for determine whether a page is already locked by a transaction)

This lab mainly implement the a lockManager to manage locks on page, to be able to acquire/upgrade/release lock when possible (no deadlock). And make some adjustment to the bufferPool to make it support the concurrency features:

- LockManager: 

  When I implement this part, I does not implement a class of lock specifically, instead, I implement a page unit of locks to make the interaction of other function more straightforward (many manipulation of locks on page are organized on a single page level.) And my implementation of acquire is pretty straightforward, the function will repeatedly attempt to acquire lock according to the permission, and if no deadlock is detected. To decide when and how to acquire which kind of lock, I use nested if conditions to first decide whether there is a exclusive lock on a page, and then if the current transaction need read_write permission, it might only choose to upgrade, and if it need read permission, it can safe to acquire if no exclusive lock on that page. And for deadlock detection, I use both the policies of dependency graph and timeout. For dependency graph, I keep a map of the wait-for relationship between pairs of transactions and use a breadth-first search based algorithm to test whether the graph is acyclic. 

- BufferPool:

  All the changes are pretty straight forward, just add the support for the lock system. These change includes add the lock acquire before read a page; detect whether the page are dirty before evict a page; flush and release all lock when transaction is completed whether it is committed or abort.

### Design decisions (Perhap a chance for bonus?):

As I mentioned above, I attempted to implement both policy for deadlock detection. However, none of the two version finished all the test case. But dependency graphs pass more test case compare to timeouts policy, in a much faster speed. The detail of lock manager design are stated above. 

### Extra unit test:

- I suggest to add a unit test to check whether the lock is added by same transaction when evict and acquire. Because initially I encounter with this issue but unit test case does not address that issue. So some time are wasted when I debugging the deadlock detection.

### Api  change:

​	no change

### Missing or incomplete elements of your code: 

​	No missing elements, but failed testTenThreads and testFiveThreads two test cases, I am not sure where are the problems in my code. I test and the deadlock detection algorithm can correctly detect deadlock and abort correspondingly. 

