package simpledb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;
import simpledb.*;

public class AlternativeCardEstTest  extends SimpleDbTestBase {

    /**
     * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create
     * an identical HeapFile table
     * 
     * @param tuples
     *            Tuples to create a HeapFile from
     * @param columns
     *            Each entry in tuples[] must have
     *            "columns == tuples.get(i).size()"
     * @param colPrefix
     *            String to prefix to the column names (the columns are named
     *            after their column number by default)
     * @return a new HeapFile containing the specified tuples
     * @throws IOException
     *             if a temporary file can't be created to hand to HeapFile to
     *             open and read its data
     */
    public static HeapFile createDuplicateHeapFile(
            ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix)
            throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.getPageSize(), columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
    }

    ArrayList<ArrayList<Integer>> tuples3;
    HeapFile f3;
    String tableName3;
    int tableId3;
    TableStats stats3;

    ArrayList<ArrayList<Integer>> tuples4;
    HeapFile f4;
    String tableName4;
    int tableId4;
    TableStats stats4;

    /**
     * Set up the test; create some initial tables to work with
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create some sample tables to work with
        //create table 3 with small range, so that distinct number of value can be regard as max value
        this.tuples3 = new ArrayList<ArrayList<Integer>>();
        this.f3 = SystemTestUtil.createRandomHeapFile(10, 10000, 5, null,
                tuples3, "c");

        this.tableName3 = "TC";
        Database.getCatalog().addTable(f3, tableName3, "c1");
        this.tableId3 = Database.getCatalog().getTableId(tableName3);
        System.out.println("tableId3: " + tableId3);

        stats3 = new TableStats(tableId3, 19);

        TableStats.setTableStats(tableName3, stats3);


        //create table 4, alike table 3
        this.tuples4 = new ArrayList<ArrayList<Integer>>();
        this.f4 = SystemTestUtil.createRandomHeapFile(10, 10000, 5, null,
                tuples4, "c");

        this.tableName4 = "TD";
        Database.getCatalog().addTable(f4, tableName4, "c1");
        this.tableId4 = Database.getCatalog().getTableId(tableName4);
        System.out.println("tableId4: " + tableId4);

        stats4 = new TableStats(tableId4, 19);

        TableStats.setTableStats(tableName4, stats4);
    }

    private double[] getRandomJoinCosts(JoinOptimizer jo, LogicalJoinNode js,
            int[] card1s, int[] card2s, double[] cost1s, double[] cost2s) {
        double[] ret = new double[card1s.length];
        for (int i = 0; i < card1s.length; ++i) {
            ret[i] = jo.estimateJoinCost(js, card1s[i], card2s[i], cost1s[i],
                    cost2s[i]);
            // assert that he join cost is no less than the total cost of
            // scanning two tables
            Assert.assertTrue(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }

    @Test
    public void estimateJoinOfNoKeyCardinality() throws ParsingException {
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
        System.out.println( "SELECT * FROM " + tableName3 + " t3, " + tableName4
                + " t4 WHERE t3.c3 = t4.c4;");

        JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid,
                "SELECT * FROM " + tableName3 + " t3, " + tableName4
                        + " t4 WHERE t3.c3 = t4.c4;"),
                new Vector<LogicalJoinNode>());

        double cardinality;

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t3", "t4",
                        "c" + Integer.toString(3), "c" + Integer.toString(4),
                        Predicate.Op.EQUALS), stats3.estimateTableCardinality(0.4),
                stats4.estimateTableCardinality(0.5), false, false, TableStats
                        .getStatsMap());
        // On a primary key, the cardinality is well-defined and exact (should
        // be size of fk table)
        // BUT we had a bug in lab 4 in 2009 that suggested should be size of pk
        // table, so accept either
        System.out.println(cardinality);
        Assert.assertTrue(cardinality == (double) 4000 * 5000 * 1/5);
    }

    /**
     * Determine whether the orderJoins implementation is doing a reasonable job
     * of ordering joins, and not taking an unreasonable amount of time to do so
     */
    @Test
    public void orderJoinsTest() throws ParsingException, IOException,
            DbException, TransactionAbortedException {
        // This test is intended to approximate the join described in the
        // "Query Planning" section of 2009 Quiz 1,
        // though with some minor variation due to limitations in simpledb
        // and to only test your integer-heuristic code rather than
        // string-heuristic code.

        final int IO_COST = 101;

        // Create a whole bunch of variables that we're going to use
        TransactionId tid = new TransactionId();
        JoinOptimizer j;
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();

        // Create all of the tables, and add them to the catalog
        ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null,
                empTuples, "c");
        Database.getCatalog().addTable(emp, "emp");

        ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null,
                deptTuples, "c");
        Database.getCatalog().addTable(dept, "dept");

        ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null,
                hobbyTuples, "c");
        Database.getCatalog().addTable(hobby, "hobby");

        ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null,
                hobbiesTuples, "c");
        Database.getCatalog().addTable(hobbies, "hobbies");

        // Get TableStats objects for each of the tables that we just generated.
        stats.put("emp", new TableStats(
                Database.getCatalog().getTableId("emp"), IO_COST));
        stats.put("dept",
                new TableStats(Database.getCatalog().getTableId("dept"),
                        IO_COST));
        stats.put("hobby",
                new TableStats(Database.getCatalog().getTableId("hobby"),
                        IO_COST));
        stats.put("hobbies",
                new TableStats(Database.getCatalog().getTableId("hobbies"),
                        IO_COST));

        // Note that your code shouldn't re-compute selectivities.
        // If you get statistics numbers, even if they're wrong (which they are
        // here
        // because the data is random), you should use the numbers that you are
        // given.
        // Re-computing them at runtime is generally too expensive for complex
        // queries.
        filterSelectivities.put("emp", 0.1);
        filterSelectivities.put("dept", 1.0);
        filterSelectivities.put("hobby", 1.0);
        filterSelectivities.put("hobbies", 1.0);

        // Note that there's no particular guarantee that the LogicalJoinNode's
        // will be in
        // the same order as they were written in the query.
        // They just have to be in an order that uses the same operators and
        // semantically means the same thing.
        nodes.add(new LogicalJoinNode("hobbies", "hobby", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "dept", "c1", "c0",
                Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("emp", "hobbies", "c2", "c0",
                Predicate.Op.EQUALS));
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND e.c3 < 1000;"),
                nodes);

        // Set the last boolean here to 'true' in order to have orderJoins()
        // print out its logic
        result = j.orderJoins(stats, filterSelectivities, false);

        // There are only three join nodes; if you're only re-ordering the join
        // nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // There were a number of ways to do the query in this quiz, reasonably
        // well;
        // we're just doing a heuristics-based optimizer, so, only ignore the
        // really
        // bad case where "hobbies" is the outermost node in the left-deep tree.
        Assert.assertFalse(result.get(0).t1Alias == "hobbies");

        // Also check for some of the other silly cases, like forcing a cross
        // join by
        // "hobbies" only being at the two extremes, or "hobbies" being the
        // outermost table.
        Assert.assertFalse(result.get(2).t2Alias == "hobbies"
                && (result.get(0).t1Alias == "hobbies" || result.get(0).t2Alias == "hobbies"));
    }

    /**
     * Test a much-larger join ordering, to confirm that it executes in a
     * reasonable amount of time
     */
    @Test(timeout = 60000)
    public void bigOrderJoinsTest() throws IOException, DbException,
            TransactionAbortedException, ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100,
                Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");

        ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        for (int i = 0; i < 100000; i++) {
            bigHeapFileTuples.add(smallHeapFileTuples.get(i % 100));
        }
        HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2,
                "c");
        Database.getCatalog().addTable(bigHeapFile, "bigTable");

        // Add the tables to the database
        Database.getCatalog().addTable(bigHeapFile, "bigTable");
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");
        Database.getCatalog().addTable(smallHeapFileJ, "j");
        Database.getCatalog().addTable(smallHeapFileK, "k");
        Database.getCatalog().addTable(smallHeapFileL, "l");
        Database.getCatalog().addTable(smallHeapFileM, "m");
        Database.getCatalog().addTable(smallHeapFileN, "n");

        // Come up with join statistics for the tables
        stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("j", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("k", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("l", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("m", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("n", new TableStats(smallHeapFileG.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("bigTable", 1.0);
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);
        filterSelectivities.put("j", 1.0);
        filterSelectivities.put("k", 1.0);
        filterSelectivities.put("l", 1.0);
        filterSelectivities.put("m", 1.0);
        filterSelectivities.put("n", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("i", "j", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("j", "k", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("k", "l", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("l", "m", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("m", "n", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("n", "bigTable", "c0", "c0",
                Predicate.Op.EQUALS));

        // Make sure we don't give the nodes to the optimizer in a nice order
        Collections.shuffle(nodes);
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;"),
                nodes);

        // Set the last boolean here to 'true' in order to have orderJoins()
        // print out its logic
        result = j.orderJoins(stats, filterSelectivities, false);

        // If you're only re-ordering the join nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // Make sure that "bigTable" is the outermost table in the join
        Assert.assertEquals(result.get(result.size() - 1).t2Alias, "bigTable");
    }


}
