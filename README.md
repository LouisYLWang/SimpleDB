SimpleDB architecture - course project report of CSE 444

 <img src="https://documents.app.lucidchart.com/documents/5fa8f51e-2b02-48c0-b57e-6b54e66cc06c/pages/0_0?a=5970&amp;x=-81&amp;y=-76&amp;w=2605&amp;h=1971&amp;store=1&amp;accept=image%2F*&amp;auth=LCA%207ea37ae89e2da99b1f998ba9186e367a01470f45-ts%3D1592300798" alt="img" style="zoom: 25%;" />

## Lab 1 ##

### Big pictures:

-  The whole picture is what this lab finished is the core four layers architecture of the SimpleDB, when the query was executed, it call corresponding OpIterator, which calls HeapPage to get corresponding page which calls HeapFile to get corresponding files from BufferPool. And BufferPool is like a proxy, and if the file to read is not already in BufferPool, it will read from disk and evict other page if it full. Basically it is like a chain of iterators and call from top to bottom, and query individual tuple.

### Components introduction:

-  Tuple & TupleDesc: Implement to make DbPage & Dbfile manager class and operator to be able identify the field of each tuple / table schema. The type of each field are crucial for the calculation of  page header size, page size and slots number;
-  Catalog: Implement  to map files with table, as a registry of tables schema;
-  BufferPool: Implement to cache the page on hard drive for quicker random access; 
   - getPage() function are used for check whether the page are read already into BufferPool and if not, retrieve it into bufferpool and evict possible pages if the BufferPool is full;
-  HeapPage: Implement to organize block/page (partial information) of table. It help to keep track of header ( the occupied slots of tuple) and tuples  (the data itself).
   - getHeaderSize() & getNumTuples() function are used to calculate the total slot  to fill the page. 
   - iterator() can be called to retrieve Tuple iteratively, this are used for calls from upper layers (HeapFile, Operators) to return individual tuple once at a time.
   - I made a mistake in getHeaderSize, originally I used a float divide, which cause specific columns size failed the ScanTest, later on I found out the headersize round down by 1 when casting to integer.
-  HeapFile: Implement to manage HeapPages and organized several pages into one table.  
   - numPages(): calculate and return the number of pages in the file
   - readPage(): to read page from BufferPool, a thing worth to notice is that to double check the offset to make sure the length of page is suitable to read into the byte array by which to return a new HeaPage.
-  HeapFileIterator: Implement to allow upper layers class (Operator) to parse individual tuple once at a time. I included this class in the HeapFile class in order to avoid the unnecessary pass of HeapFile to the HeapFileIterator as a parameter for constructor.
-  SeqScan: the most basic operator, implement to linear scan through the table;
   - getTupleDesc(): only thing need to beware is to add the alias before the original field name;
-  ID classes (HeapPageId, RecordId...): simply used for identifying corresponding objects, mostly only include getter() / setter() / hashcode() / equal() 

-  -  Tuple & TupleDesc: Implement to make DbPage & Dbfile manager class and operator to be able identify the field of each tuple / table schema. The type of each field are crucial for the calculation of  page header size, page size and slots number;
   -  Catalog: Implement  to map files with table, as a registry of tables schema;
   -  BufferPool: Implement to cache the page on hard drive for quicker random access; 
      - getPage() function are used for check whether the page are read already into BufferPool and if not, retrieve it into bufferpool and evict possible pages if the BufferPool is full;
   -  HeapPage: Implement to organize block/page (partial information) of table. It help to keep track of header ( the occupied slots of tuple) and tuples  (the data itself).
      - getHeaderSize() & getNumTuples() function are used to calculate the total slot  to fill the page. 
      - iterator() can be called to retrieve Tuple iteratively, this are used for calls from upper layers (HeapFile, Operators) to return individual tuple once at a time.
      - I made a mistake in getHeaderSize, originally I used a float divide, which cause specific columns size failed the ScanTest, later on I found out the headersize round down by 1 when casting to integer.
   -  HeapFile: Implement to manage HeapPages and organized several pages into one table.  
      - numPages(): calculate and return the number of pages in the file
      - readPage(): to read page from BufferPool, a thing worth to notice is that to double check the offset to make sure the length of page is suitable to read into the byte array by which to return a new HeaPage.
   -  HeapFileIterator: Implement to allow upper layers class (Operator) to parse individual tuple once at a time. I included this class in the HeapFile class in order to avoid the unnecessary pass of HeapFile to the HeapFileIterator as a parameter for constructor.
   -  SeqScan: the most basic operator, implement to linear scan through the table;
      - getTupleDesc(): only thing need to beware is to add the alias before the original field name;
   -  ID classes (HeapPageId, RecordId...): simply used for identifying corresponding objects, mostly only include getter() / setter() / hashcode() / equal() 

## Lab 2

### Components introduction:

-  Predicate & Join Predicate : predicate is like a encapsulation of the operands and the comparative operator to field(s) that operations call to make specific comparison as condition. Predicate is for uniary comparison for filter operation and join predicate is for binary comparison for join operation.
-  Filter: operation to filter out tuples. It iteratively fetch next tuple from single child OpIterator and use predication.filter to test the if the condition is hit for the tuples. 
-  Join: operation to merge tuples together. It iteratively fetch next tuple from the two children OpIterator and join the tuple together from calling JoinPredication. I implement two way of join, and if the join condition is equal, the function uses hash join and if the join condition is comparative, the function uses nested loop join.
-  Aggregate / IntegerAggregator / StringAggregator: operation to get aggregated value of a relation. groupby is the operation to define the observation group of each aggregate calculation. As the other operator, it fetch next tuple from it child and update the aggregated value to the temp store (hashmap).
-  Insertion / deletion: insert and delete are two key operator to make db to modify tuple in tables. The lab implement three levels of insertion/deletion operation from tuple to file (to bufferpool) to page. For each level, we need to specifically deal with the effect of insert or delete (change header page correspondingly at page level, trace the modified page and add/remove page at file level, and reflect the change at bufferpool and evict page if bufferpool is full etc.)
-  sideworks: to prepare for the transaction and concurrent features, the lab also add dirty label and release page back to disk. For evict policy, I choose the randomized eviction.

### Design decisions: 

-  As I mentioned above, I choose a combination of nested loop join and hash join, and I choose randomized page eviction policy. The trade-off of adding hash join along side with nested loop join is space, because when the join operator is opened, whether the future join condition is equal or not, my solution will always read child 1 into a hashMap, and add an queue for outputBuffer, which will cost more space in exchange for a quicker equal-condition join.

## Lab 3

### Big pictures:

-  The main goal of Lab 3 is to implement the Transactions functionality of the simple DB using NO STEAL and FORCE  buffer management policy in the granularity of page level. NO STEAL means the eviction of dirty pages only happened after a transaction is committed. FORCE means the dirty pages are forced to flush to disk after a transaction is committed. This lab mainly implement the a lockManager to manage locks on page, to be able to acquire/upgrade/release lock when possible (no deadlock). And make some adjustment to the bufferPool to make it support the concurrency features:

### Components introduction:

-  LockManager:
   - LockOnPage: class to organize locks on specific page
   - locks hashmap: locks store - link page to LockOnPage, use for find lock on specific page
   - transaction hashmap: transaction store - link transaction to page it lock
   - waitForGraph: use for deadlock detection
   - acquire / upgrade: acquire read/write lock and upgrade read lock 
   - detectDeadlock(): detect deadlock
   - release(): release all lock of a transaction
-  BufferPool:
   - getPage(): add aquire lock before getting file from disk
   - transactionComplete: NO STEAL and FORCE part of policy
   - adjustment of evictPage: NO STEAL part of policy
   - *releasePage()
   - *holdsLock() (does not use this function for determine whether a page is already locked by a transaction)

### Design decisions:

- LockManager: 

  When I implement this part, I does not implement a class of lock specifically, instead, I implement a page unit of locks to make the interaction of other function more straightforward (many manipulation of locks on page are organized on a single page level.) And my implementation of acquire is pretty straightforward, the function will repeatedly attempt to acquire lock according to the permission, and if no deadlock is detected. To decide when and how to acquire which kind of lock, I use nested if conditions to first decide whether there is a exclusive lock on a page, and then if the current transaction need read_write permission, it might only choose to upgrade, and if it need read permission, it can safe to acquire if no exclusive lock on that page. And for deadlock detection, I use both the policies of dependency graph and timeout. For dependency graph, I keep a map of the wait-for relationship between pairs of transactions and use a breadth-first search based algorithm to test whether the graph is acyclic. 

- BufferPool:

  All the changes are pretty straight forward, just add the support for the lock system. These change includes add the lock acquire before read a page; detect whether the page are dirty before evict a page; flush and release all lock when transaction is completed whether it is committed or abort.

## Lab 4

### Big pictures:

-  The main goal of Lab 4 is to implement the log-based rollback and recovery. This lab changes the buffer management policy to FORCE to NOFORCE, that means the dirty page will not be forced to flush to disk after commit. This give simpleDB more flexibility and efficiency to avoid the unnecessary output to disk. And inhered from the page-level lock management, the logging also happen on whole page, that means if a page is modified by a update operation, the after image of the whole page will be logged to the record. 

### Components introduction:

-  Rollback
   - CLR record: define a new type of record consists of
     - record type (INT)
     - transaction id (LONG)
     - undo target log start offset (LONG)
     - start offset (LONG)
   - undo: iterate through the log and setting the state of specific  pages it updated to their pre-updated state
-  Recover: recover the commit state of the while database, redo the committed page and undo the uncommitted page
   - redo: iterate through the log and setting the state of specific  pages it updated to their updated state
-  Other:
   - BufferPool change to accommodate NOFORCE policy:
     - evictPage(): remove the dirty-page check before flushing a page to make more space
     - transactionComplete(): add logging of the writeLog

  All the changes are pretty straight forward, just add the support for the lock system. These change includes add the lock acquire before read a page; detect whether the page are dirty before evict a page; flush and release all lock when transaction is completed whether it is committed or abort.

-  CLR record: 
   - I does not follows the pattern of logWrite, which write the whole page images before and after to the log. Instead, I just write offset of the target undo record to the CLR record (as in the slide), so the file pointer will temporarily seek back to the offset before and undo the change.
-  Undo:
   - I merged the undo of single transaction in rollback and the undo of multiple transactions in recovery together
   - To achieve the forward scan followed by a backward undo, I define a CompensateRecord class to encapsulate the transactionID, offset of the target undo record and page image together so that it can bind together and easy to manipulate by stack. 

## Lab 5 (pending..)

