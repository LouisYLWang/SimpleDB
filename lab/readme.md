Yiliang Wang 

CSE 444 

LAB 2 WRITE-UP

### Runtime of queries

Since I am not a student of CSE section, I can not get access to *attu*, hence the runtime are all from my laptop (i7-10510U 16GB RAM)

- **query 1** 0.41s
- **query 2** 1.11s
- **query 3** 1.52s

- **query 1** 

![image-20200502191234875](C:\Users\louis\AppData\Roaming\Typora\typora-user-images\image-20200502191234875.png)

- **query 2** 

![image-20200502191207389](C:\Users\louis\AppData\Roaming\Typora\typora-user-images\image-20200502191207389.png)

- **query** 3

![image-20200502191300839](C:\Users\louis\AppData\Roaming\Typora\typora-user-images\image-20200502191300839.png)

### Demonstrate your understanding

The main goal of Lab 2 is to implement the operators & aggregators, whole picture is:

- Relational algebra: 

  - Filter
  - Join
  - Aggregates
    - Integer & String 

- Mutability: 

  - Insertion 
    - eviction 
  - Deletion 

  

This lab mainly implement the skeletons functions of main components of SimpleDB to enable the database to achieve bellowed functionalities: 

- Predicate & Join Predicate : predicate is like a encapsulation of the operands and the comparative operator to field(s) that operations call to make specific comparison as condition. Predicate is for uniary comparison for filter operation and join predicate is for binary comparison for join operation.
- Filter: operation to filter out tuples. It iteratively fetch next tuple from single child OpIterator and use predication.filter to test the if the condition is hit for the tuples. 
- Join: operation to merge tuples together. It iteratively fetch next tuple from the two children OpIterator and join the tuple together from calling JoinPredication. I implement two way of join, and if the join condition is equal, the function uses hash join and if the join condition is comparative, the function uses nested loop join.
- Aggregate / IntegerAggregator / StringAggregator: operation to get aggregated value of a relation. groupby is the operation to define the observation group of each aggregate calculation. As the other operator, it fetch next tuple from it child and update the aggregated value to the temp store (hashmap).
- Insertion / deletion: insert and delete are two key operator to make db to modify tuple in tables. The lab implement three levels of insertion/deletion operation from tuple to file (to bufferpool) to page. For each level, we need to specifically deal with the effect of insert or delete (change header page correspondingly at page level, trace the modified page and add/remove page at file level, and reflect the change at bufferpool and evict page if bufferpool is full etc.)
- sideworks: to prepare for the transaction and concurrent features, the lab also add dirty label and release page back to disk. For evict policy, I choose the randomized eviction.

### Design decisions:

As I mentioned above, I choose a combination of nested loop join and hash join, and I choose randomized page eviction policy. The trade-off of adding hash join along side with nested loop join is space, because when the join operator is opened, whether the future join condition is equal or not, my solution will always read child 1 into a hashMap, and add an queue for outputBuffer, which will cost more space in exchange for a quicker equal-condition join.

### Extra unit test:

- Actually because of the error when I add the Query Parser, I found a bug in Catalog which does not been tested in test cases.  My tableIdIterator() method had a wrong return condition, so maybe is better to cover the test for tableIdIterator in unit test for better debugging in the future.
- It makes me very confused to the mismatch between aggregate unit test case and the java doc. In Aggregate  getTupleDesc(), we are required to make aggregate column name informative, so to meet this requirement, I combined the name of afield with Aggregator operations type, but it makes me failed the test case. I guess it probably because there are null  TupleDesc for the child level, and the TupleDesc constructor allows for null field name. But at this case, my aggregate field informatively got the name of "sum(null)", and it is conflict with the assertion condition in aggregate unit test. If the test case considers this situation, it would be better for debugging in the future.

### Api  change:

​	no change

### Missing or incomplete elements of your code: 

​	No missing elements for lab 2

