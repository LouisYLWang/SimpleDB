Yiliang Wang 

CSE 444 

LAB 1

### Demonstrate your understanding

This lab mainly implement the skeletons functions of main components of SimpleDB to enable the database to achieve bellowed functionalities: 

-  Tuple & TupleDesc: Implement to make DbPage & Dbfile manager class and operator to be able identify the field of each tuple / table schema. The type of each field are crucial for the calculation of  page header size, page size and slots number;
- Catalog: Implement  to map files with table, as a registry of tables schema;
- BufferPool: Implement to cache the page on hard drive for quicker random access; 
  - getPage() function are used for check whether the page are read already into BufferPool and if not, retrieve it into bufferpool and evict possible pages if the BufferPool is full;
- HeapPage: Implement to organize block/page (partial information) of table. It help to keep track of header ( the occupied slots of tuple) and tuples  (the data itself).
  - getHeaderSize() & getNumTuples() function are used to calculate the total slot  to fill the page. 
  - iterator() can be called to retrieve Tuple iteratively, this are used for calls from upper layers (HeapFile, Operators) to return individual tuple once at a time.
  - I made a mistake in getHeaderSize, originally I used a float divide, which cause specific columns size failed the ScanTest, later on I found out the headersize round down by 1 when casting to integer.
- HeapFile: Implement to manage HeapPages and organized several pages into one table.  
  - numPages(): calculate and return the number of pages in the file
  - readPage(): to read page from BufferPool, a thing worth to notice is that to double check the offset to make sure the length of page is suitable to read into the byte array by which to return a new HeaPage.
- HeapFileIterator: Implement to allow upper layers class (Operator) to parse individual tuple once at a time. I included this class in the HeapFile class in order to avoid the unnecessary pass of HeapFile to the HeapFileIterator as a parameter for constructor.
- SeqScan: the most basic operator, implement to linear scan through the table;
  - getTupleDesc(): only thing need to beware is to add the alias before the original field name;
- ID classes (HeapPageId, RecordId...): simply used for identifying corresponding objects, mostly only include getter() / setter() / hashcode() / equal() 

The whole picture is what this lab finished is the core four layers architecture of the SimpleDP, when the query was executed, it call corresponding OpIterator, which calls HeapPage to get corresponding page which calls HeapFile to get corresponding files from BufferPool. And BufferPool is like a proxy, and if the file to read is not already in BufferPool, it will read from disk and evict other page if it full. Basically it is like a chain of iterators and call from top to bottom, and query individual tuple.

### Extra unit test:

Maybe the unit test can also include the check for error handling, for example, the readPage() / getPageData() in both HeapFile and HeapPage need to check wether the error is handled according to the document.

### Api  change:

 - In TupleDesc:
    - add a alternative constructor that takes in  TDItem array and turn it directly into TupleDesc without loop over both fieldName and fieldType array; The benefit is when update the tupleDesc in SeqScan, it might be a little bit more straightforward to not unzip two array which used to create new tupleDesc;
 - In HeapFileIterator: 
   	- add a function openByPgNum() to avoid duplicated code when retrieve next page in hasNext();

### Missing or incomplete elements of your code: 

​	No missing elements for lab 1

