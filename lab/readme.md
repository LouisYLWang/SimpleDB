Yiliang Wang 

CSE 444 

LAB 4: Rollback and Recovery

### Demonstrate your understanding

The main goal of Lab 4 is to implement the log-based rollback and recovery. This lab changes the buffer management policy to FORCE to NOFORCE, that means the dirty page will not be forced to flush to disk after commit. This give simpleDB more flexibility and efficiency to avoid the unnecessary output to disk. And inhered from the page-level lock management, the logging also happen on whole page, that means if a page is modified by a update operation, the after image of the whole page will be logged to the record. 

The whole picture of the lab is:

- Rollback
  - CLR record: define a new type of record consists of
    - record type (INT)
    - transaction id (LONG)
    - undo target log start offset (LONG)
    - start offset (LONG)
  - undo: iterate through the log and setting the state of specific  pages it updated to their pre-updated state

- Recover: recover the commit state of the while database, redo the committed page and undo the uncommitted page
  - redo: iterate through the log and setting the state of specific  pages it updated to their updated state
- Other:
  - BufferPool change to accommodate NOFORCE policy:
    - evictPage(): remove the dirty-page check before flushing a page to make more space
    - transactionComplete(): add logging of the writeLog

### Design decisions:

- CLR record: 
  - I does not follows the pattern of logWrite, which write the whole page images before and after to the log. Instead, I just write offset of the target undo record to the CLR record (as in the slide), so the file pointer will temporarily seek back to the offset before and undo the change.
- Undo:
  - I merged the undo of single transaction in rollback and the undo of multiple transactions in recovery together
  - To achieve the forward scan followed by a backward undo, I define a CompensateRecord class to encapsulate the transactionID, offset of the target undo record and page image together so that it can bind together and easy to manipulate by stack. 

### Extra unit test:

- I may contribute no more extra unite test, but I have a question, it is possible to simplify the test cases by not sequentially scan through the Heap file but just compare the expect and the actual log file? I think both way should work and scan the log   may avoid some input from disk. (Oh I see, the test cases need to provide tuple level test)

### API  change:

- public class add: logCompensate
- public method add: undo

### Missing or incomplete elements of your code: 

​	No missing elements

