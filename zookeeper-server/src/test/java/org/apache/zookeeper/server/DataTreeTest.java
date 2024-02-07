package org.apache.zookeeper.server;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.slf4j.Logger;

@RunWith(Enclosed.class)
public class DataTreeTest {
    private static Random random = new Random(System.currentTimeMillis());
	@RunWith(Parameterized.class)
	public static class CreateNodeTest {
		// Input data
	    private String path;
	    private byte[] data;
	    private List<ACL> acl;
	    private long ephemeralOwner;
	    private int parentCVersion;
	    private long zxid;
	    private long time;
	    
	    // output data
	    private String expectedOutput;

	    // old variables for testing
	    private int oldParCVers;
	    private long oldParZxid;
	    private int oldSizeCont;
	    
        private Stat stat;
	    private DataTree dataTree;
	    private String[] pathNodes;
	    private int typeTest;
	    private int nNodes;

	    public CreateNodeTest(String path, int numData, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
			configure(path, numData, acl, ephemeralOwner, parentCVersion, zxid, time, typeTest, expectedOutput);
	    }	
	    	
	    public void configure(String path, int numData, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
	    	/*
	    	 * The test behavior change for different typeTest
	    	 * 	1: Add parents nodes
	    	 * 	2: Father node is not added 
	    	 * 	3: Father node is ephemeral
	    	 * 	4: Add node two times
	    	 * 	5: Pass stat (not null) parameter
	    	 *  6: Add quota limit node
	    	 *  7: Add stat limit node
	    	 */
	    	
	    	// input values
	    	this.path = path;
	        this.acl = acl;
	        this.ephemeralOwner = ephemeralOwner;
	        this.parentCVersion = parentCVersion;
	        this.zxid = zxid;
	        this.time = time;
	        this.typeTest = typeTest;
	        this.expectedOutput = expectedOutput;
	        
	        stat = new Stat();
	        long isFatherEph = 0;
            this.dataTree = new DataTree();

	        if(numData >= 0) {
		        this.data = new byte[numData];
	            random.nextBytes(this.data);
	        }
	        else this.data = null;
	        
	        // Old values
	        this.oldParCVers = 0;
	        this.oldParZxid = 1;
	        this.oldSizeCont = dataTree.getContainers().size();
	        // Set DataTree pre tests. Add all father nodes
            if(path == null) {
            	pathNodes = null;
            	nNodes = 0;
            	return;
            }
            this.pathNodes = path.split("/");            
            int n = pathNodes.length;
            String currentPath;
            String prevPath = "/";
            int count = 1;
            for (String pathElement : pathNodes) {
                currentPath = prevPath + pathElement;
                if (count == (n)) {
                    break;
                }
                if (typeTest == 2) continue;
                if (typeTest == 3) isFatherEph = -1;
                if ((typeTest == 6 ||typeTest == 7)  && count == 1) currentPath = Quotas.quotaZookeeper+currentPath;    
                this.dataTree.createNode(currentPath, new byte[100], null, isFatherEph, dataTree.getNode(Quotas.quotaZookeeper).stat.getCversion(), this.oldParCVers, this.oldParZxid);
                prevPath = currentPath;
                if (count != 1) {
                    prevPath += "/";
                    count++;
                    continue;
                }
                count++;
            }
            this.nNodes = n;
            if (typeTest == 6 || typeTest == 7) this.nNodes++;
            
	    }
	
	    @Parameters
	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
	            // path, numData, acl, ephemeralOwner, parentCVersion, zxid, time, typeTest, expectedOutput
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, -1, -1, -1, -1, 1, ""},										// 0
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0, 0, 0, 0, 1, ""},											// 1
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xff00000000000001L, 1, 1, 1, 1, ""},						// 2: TTL node
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0x8000000000000000L, 5, 3, 1000, 1, ""},					// 3: Container node
                {"/node1", 10, ZooDefs.Ids.OPEN_ACL_UNSAFE, 1, 0, 1, 1, 1, ""},											// 4: ACL open
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 1, 0, 1, 1, 1, ""},											// 5: ACL read
                {"/node1", 10, new ArrayList<>(), 1, 0, 1, 1, 1, ""},													// 6: ACL empty
                {"/node1", 10, null, 1, 0, 1, 1, 1, ""},																// 7: ACL null
                {"/node1", 0, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, ""},											// 8: 0 datas
                {"/node1", -1, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NullPointerException"},						// 9: -1 datas       
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 1, 1, 1000, 1, ""},								// 10:         
                {"node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "StringIndexOutOfBoundsException"},			// 11: Incorrect path       
                {"node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NoNodeException"},						// 12: incorrect father path         
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 2, "NoNodeException"}, 					// 13: Father node is not added 
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 3, "NoNodeException"}, 	 				// 14: Father is ephemeral node        
                {null, 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NullPointerException"},							// 15: null path         
                {"", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "StringIndexOutOfBoundsException"},				// 16: empty path
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, -1, -1, -1, 1000, 1, ""},									// 17: ephemeral and 1000 times
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 4, "NodeExistsException"},  					// 18: Add node two times
                
                // Add after analisys
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xff00000000000001L, -1, 2, -1, 5, ""},					    // 19: Add Stat as parameter
                // add quota
                {"/node1/"+Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},						// 20: Quota limit node
                {"/node1/"+Quotas.statNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 7, ""},						// 21: Quota stat node
                {"/node1/"+Quotas.limitNode + "/node2", 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},			// 22: Quota limit node with son
                {"/node1/"+ Quotas.limitNode + "/" + Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},	// 23: Two quota limit node
	        });
	    }
	    
	    @Test(timeout=1000)
	    public void createNodeTest() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
	    	try{	    		
	    		// start method
		    	if(typeTest == 5) stat = new Stat();
		    	if(typeTest == 6 || typeTest == 7) path = Quotas.quotaZookeeper + path;
	    		if (typeTest == 4) dataTree.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, stat);
	    		dataTree.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, stat);
	    		
	    		// Get State
	    		String parentPath = getParentString(path);
		        DataNode createdNode = dataTree.getNode(path);
//		        DataNode parentNode = dataTree.getNode(parentPath);		        
		        
		        // Asserts 
		        assertNotNull(createdNode);
		        assertEquals(this.nNodes, dataTree.getNodeCount()-4);
		        if(acl!= null) assertEquals(acl, dataTree.getACL(createdNode));
		        if(ephemeralOwner != 0x8000000000000000L) assertEquals(ephemeralOwner, stat.getEphemeralOwner());
		        assertEquals(zxid, stat.getCzxid());
		        assertEquals(time, stat.getCtime());
		        if(typeTest != 7) {
		        	assertArrayEquals(data, createdNode.getData());
		        	assertEquals(data.length, stat.getDataLength());
		        }
		        if(ephemeralOwner == 0x8000000000000000L) assertEquals(oldSizeCont+1, dataTree.getContainers().size());
		        
		        // Assert parent state
		        if(parentPath != "/" && typeTest == 1) {
		        	long parPZxid = this.oldParZxid;
		        	int parCVersion = this.oldParCVers+1;
		        	if(parentCVersion > this.oldParCVers) {
		        		parCVersion = parentCVersion;
		        		parPZxid = zxid;
		        	}
			        assertEquals(this.parentCVersion, parCVersion);
			        assertEquals(this.zxid, parPZxid);
		        }
		        
	    	} catch (Exception e) {
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
	    	}
	    }	       
	   
	}
	
	
	@RunWith(Parameterized.class)
	public static class DeleteNodeTest {
		// Input data
	    private String path;
        private long zxid;

	    // output data
	    private String expectedOutput;

	    // variables for testing
	    private DataTree dataTree;
	    private int typeTest;
	    private int nNodes = 1;
	    private String parentPath;
	    
	    @Mock
	    private Logger LOG;
	    
	    public DeleteNodeTest(String path, long zxid, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, NodeExistsException{
			configure(path, zxid, typeTest, expectedOutput);
	    }	
	    	
	    public void configure(String path, long zxid, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, NodeExistsException {
	    	/*
	    	 * The test behavior change for different typeTest
	    	 * 	1: Add nodes
	    	 * 	2: Add limit node quota
	    	 * 	3: Add stat node quota
	    	 * 	4: Add ephemeral father
	    	 * 	5: Add TTL node
	    	 *  6: Add Container node
	    	 *  7: Node already eliminated
	    	 *  8: Father node eliminated
	    	 */
	    	this.path = path;
	        this.zxid = zxid;
	        this.typeTest = typeTest;
	        this.expectedOutput = expectedOutput;
	        this.dataTree = new DataTree();
	        
	        if(typeTest == 1|| typeTest == 7 || typeTest == 8) {
	        	this.dataTree.createNode("/node1", new byte[100], null, 0,  0, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, 0,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 2) {
	        	this.dataTree.createNode(Quotas.quotaZookeeper+"/node1", new byte[100], null, 0, 0, 0, 1);
	        	this.dataTree.createNode(Quotas.quotaZookeeper+"/node1/"+ Quotas.limitNode, new byte[100], null, 0,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 3) {
	        	this.dataTree.createNode(Quotas.quotaZookeeper+"/node1", new byte[100], null, 0,  0, 0, 1);
	        	this.dataTree.createNode(Quotas.quotaZookeeper+"/node1/"+ Quotas.statNode, new byte[100], null, 0,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 4) {
	        	this.dataTree.createNode("/node1", new byte[100], null, -1,  5, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, 1,  5, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 5) {
	        	this.dataTree.createNode("/node1", new byte[100], null, 1,  0, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, 0xFF00000000000001L,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 6) {
	        	this.dataTree.createNode("/node1", new byte[100], null, 0,  0, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, 0x8000000000000000L,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	    }	    
	
	    @Parameters

	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
	                {null, -1, 1, "NullPointerException"},						// 0: Path null
	                {"", 0, 1, "StringIndexOutOfBoundsException"},				// 1: Path empty
	                {"node2", 1, 1, "StringIndexOutOfBoundsException"},			// 2: Path incorrect
	                {"node1/node2", -1, 1, "NoNodeException"},					// 3: Path incorrect
	                {"/node2", 1, 1, "NoNodeException"},						// 4: Nodo non esistente
	                {"/node1/node2", 0, 1, ""},									// 5: Op. eseguita
	                {"/node1", 1, 1, ""},										// 6: Op. eseguita
	                {"/node1/node2", 1000, 4, ""},								// 7: Nodo padre effimero. Op. eseguita
	                {"/node1/" + Quotas.limitNode, 0, 2, ""},					// 8: Nodo Quotas.limitNode. Op. eseguita
	                {"/node1/" + Quotas.statNode, 1, 3, ""},					// 9: Nodo Quotas.statNode. Op. eseguita
	                
	                // Aggiunti per Jacoco: Nodi Quota
	                {"/node1/node2", -1000, 5, ""},								// 10: Nodo TTL. Op. eseguita
	                {"/node1/node2", 1000, 6, ""},								// 11: Nodo Container. Op. eseguita
	                {"/node1/node2", 1000, 7, "NoNodeException"},				// 12: Nodo già eliminato
	                {"/node1/node2", 1000, 8, "NoNodeException"},				// 13: Nodo padre eliminato in precedenza
	        });
	    }
	    
	    @Test(timeout=1000)
	    public void deleteNodeTest(){
	    	long id;
	    	int parChildr;
	    	try{
	    		// start method 
	    		if(typeTest == 2 || typeTest == 3) path = Quotas.quotaZookeeper + path;
	    		if(typeTest == 7) {
	    			dataTree.deleteNode(path,zxid);
	    			this.nNodes--;
	    		}
		    	if(typeTest == 8) {
		    		parentPath = getParentString(path);
		    		dataTree.deleteNode(parentPath,zxid);
	    			this.nNodes--;
		    	}
		        // old values for testing   	
		        parentPath = getParentString(path);
	    		DataNode parentNode = dataTree.getNode(parentPath);
		        parChildr = dataTree.getAllChildrenNumber(parentPath);
		        int oldNodeCount = dataTree.getNodeCount();
		        long oldTreeDigest = dataTree.getTreeDigest();
		        int oldTtlsSize = dataTree.getTtls().size();
		        int oldEphemeralsCount = dataTree.getEphemeralsCount();
		        int oldContainersSize = dataTree.getContainers().size();

		        // execute function
	    		dataTree.deleteNode(path,zxid);

		        // Verify the node is deleted
		        assertEquals(this.nNodes, dataTree.getNodeCount()-3);
		        DataNode createdNode = dataTree.getNode(path);
		        assertNull(createdNode);

		        assertEquals(oldNodeCount - 1, dataTree.getNodeCount());
		        if(typeTest != 2 && typeTest != 3)assertTrue(oldTreeDigest != dataTree.getTreeDigest());
		        if(oldTtlsSize > 0) assertEquals(oldTtlsSize - 1, dataTree.getTtls().size());
		        if(oldEphemeralsCount > 0) assertEquals(oldEphemeralsCount - 1, dataTree.getEphemeralsCount());
		        if(oldContainersSize > 0) assertEquals(oldContainersSize - 1, dataTree.getContainers().size());
		        
		        
		        
		        // Verify father node info
                long parentPxzid = parentNode.stat.getPzxid();
                if(zxid > 0) id = zxid;
                else id = 0;
                assertEquals(id, parentPxzid);
                assertTrue(parChildr == dataTree.getAllChildrenNumber(parentPath)+1);

	    	} catch (Exception e) {
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
	    	}
	    }	
	}
    static String getParentString(String path){
        String[] pathNodes = path.split("/");  
        int n = pathNodes.length;
    	String p = "";
        int count = 1;
        for (String pathElement : pathNodes) {
        	if (count == n) {
            	if (count == 2) p = "/";
                break;
            }
            if(count != 1) p = p + "/";
        	p = p + pathElement;
            count++;
        }
        return p;
    }
}
