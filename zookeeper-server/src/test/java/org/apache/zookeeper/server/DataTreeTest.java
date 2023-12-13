package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@RunWith(Enclosed.class)
public class DataTreeTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);
    private static Random random = new Random(System.currentTimeMillis());
	private static int count = 0;
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

	    // variables for testing
        Stat stat;
	    private DataTree dataTree;
	    String[] pathNodes;
	    private int typeTest;
	    private int nNodes;
	
	    public CreateNodeTest(String path, int numData, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
			configure(path, numData, acl, ephemeralOwner, parentCVersion, zxid, time, typeTest, expectedOutput);
	    }	
	    	
	    public void configure(String path, int numData, List<ACL> acl, long ephemeralOwner, int parentCVersion, long zxid, long time, int typeTest, String expectedOutput) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
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

	        if(numData >= 0) {
		        this.data = new byte[numData];
	            random.nextBytes(this.data);
	        }
	        else this.data = null;

            this.dataTree = new DataTree();

            if(path == null) {
            	pathNodes = null;
            	nNodes = 0;
            	return;
            }
            this.pathNodes = path.split("/");            
            int n = pathNodes.length;
            String currentPath;
            String prevPath = "/";
            String nodeParent = "/";
            int count = 1;

            for (String pathElement : pathNodes) {
                currentPath = prevPath + pathElement;
                if (count == (n)) {
                    break;
                }
                if (typeTest == 2) continue;
                if (typeTest == 3) isFatherEph = -1;
                if ((typeTest == 6 ||typeTest == 7)  && count == 1) currentPath = Quotas.quotaZookeeper+currentPath;    
                this.dataTree.createNode(currentPath, new byte[100], null, isFatherEph, dataTree.getNode(Quotas.quotaZookeeper).stat.getCversion(), 0, 1);
                prevPath = currentPath;
                nodeParent = currentPath;
                if (count != 1) {
                    prevPath += "/";
                    count++;
                    continue;
                }
                count++;
            }
	        System.out.println(nodeParent);
            this.nNodes = n;
            if (typeTest == 6 || typeTest == 7) this.nNodes++;
	    }
	
	    @Parameters
	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, -1, -1, -1, -1, 1, ""},										// 0
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0, 0, 0, 0, 1, ""},											// 1
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xFF00000000000001L, 1, 2, 1, 1, ""},						// 2
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0x8000000000000000L, 5, 3, 1000, 1, ""},					// 3
                {"/node1", 10, ZooDefs.Ids.OPEN_ACL_UNSAFE, 1, 0, 1, 1, 1, ""},											// 4
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 1, 0, 1, 1, 1, ""},											// 5
                {"/node1", 10, new ArrayList<>(), 1, 0, 1, 1, 1, ""},													// 6
                {"/node1", 10, null, 1, 0, 1, 1, 1, ""},																// 7
                {"/node1", 0, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, ""},											// 8
                {"/node1", -1, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NullPointerException"},						// 9         
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, ""},									// 10        
                {"node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "StringIndexOutOfBoundsException"},			// 11        
                {"node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NoNodeException"},						// 12: Father path incorrect        
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 2, "NoNodeException"}, 					// 13: Father node is not added 
                {"/node1/node2", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 3, "NoNodeException"}, 	 				// 14: Father is ephemeral node        
                {null, 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "NullPointerException"},							// 15         
                {"", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 1, "StringIndexOutOfBoundsException"},				// 16         
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, -1, -1, -1, 1000, 1, ""},									// 17
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 4, "NodeExistsException"},  					// 18: Add node two times
               
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xFF00000000000001L, -1, 2, -1, 5, ""},					     // 19
                // add quora
                {"/node1/"+Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},											// 
                {"/node1/"+Quotas.statNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 7, ""},											// 
                {"/node1/"+Quotas.limitNode + "/node2", 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},
                {"/node1/"+ Quotas.limitNode + "/" + Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},											// 
	        });
	    }
	    
	    @Test(timeout=1000)
	    public void createNodeTest() throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
    		System.out.println("\n" + count);
	        count++;
	    	try{
		    	if(typeTest == 5) stat = new Stat();
		    	if(typeTest == 6 || typeTest == 7) path = Quotas.quotaZookeeper + path;
	    		dataTree.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, stat);
	    		if (typeTest == 4) dataTree.createNode(path, data, acl, ephemeralOwner, parentCVersion, zxid, time, stat);
		        DataNode createdNode = dataTree.getNode(path);
		        createdNode.copyStat(stat);
		        
		        System.out.println("ok");
		        
		        assertNotNull(createdNode);
		        assertEquals(this.nNodes, dataTree.getNodeCount()-4);
		        if(acl!= null) assertEquals(acl, dataTree.getACL(createdNode));
		        else assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, dataTree.getACL(createdNode));
		        if(ephemeralOwner != 0x8000000000000000L) assertEquals(ephemeralOwner, stat.getEphemeralOwner());
		        assertEquals(zxid, stat.getCzxid());
		        assertEquals(time, stat.getCtime());
		        if(typeTest != 7) assertArrayEquals(data, createdNode.getData());
	    	} catch (Exception e) {
	    		System.out.println("error " + e.getClass().toString());
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
	    	this.path = path;
	        this.zxid = zxid;
	        this.typeTest = typeTest;
	        this.expectedOutput = expectedOutput;
	        this.dataTree = new DataTree();
	        
	        if(typeTest == 1) {
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
	        	this.dataTree.createNode("/node1", new byte[100], null, 0,  0, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, -1,  0, 0, 1);
	        	this.nNodes = 3;
	            return;
	        }
	        if(typeTest == 5) {
	        	this.dataTree.createNode("/node1", new byte[100], null, 0,  0, 0, 1);
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
	        if(typeTest == 7) {
	        	this.dataTree.createNode("/node1", new byte[100], null, 0,  0, 0, 1);
	        	this.dataTree.createNode("/node1/node2", new byte[100], null, -1,  0, 10, 1);
	        	this.dataTree.createNode("/node1/node2/node3", new byte[100], null, -1,  10, 0, 1);
	        	this.nNodes = 4;
	            return;
	        }
	    }	    
	
	    @Parameters

	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
	                {null, -1, 1, "NullPointerException"},
	                {"", 0, 1, "StringIndexOutOfBoundsException"},
	                {"node2", 1, 1, "StringIndexOutOfBoundsException"},
	                {"node1/node2", -1, 1, "NoNodeException"},
	                {"/node1/node2", 0, 1, ""},
	                {"/node1", 1, 1, ""},
	                {"/node1/node2", 1000, 4, ""},
	                {"/node1/" + Quotas.limitNode, 0, 2, ""},
	                {"/node1/" + Quotas.statNode, 1, 3, ""},
	                
	                // Aggiunti per Jacoco
	                {"/node1/node2", -1000, 5, ""},
	                {"/node1/node2", 0, 6, ""},
	                {"/node1/node2", 0, 7, ""},
	        });
	    }
	    
	    @Test(timeout=1000)
	    public void deleteNodeTest(){
	    	long id;
    		System.out.println("\n" + count);
	        count++;
	    	try{
	    		if(typeTest == 2 || typeTest == 3) path = Quotas.quotaZookeeper + path;
		        System.out.println(path);

	    		dataTree.deleteNode(path,zxid);
	    		if(typeTest ==7) 
		        System.out.println("ok");

		        // Verify the node is deleted
		        assertEquals(this.nNodes, dataTree.getNodeCount()-3);
		        DataNode createdNode = dataTree.getNode(path);
		        assertNull(createdNode);

		        // Verify the zxid father node
	    		getParentString(path);
	    		DataNode parentNode = dataTree.getNode(parentPath);
                long parentPxzid = parentNode.stat.getPzxid();
                if(zxid > 0) id = zxid;
                else id = 0;
                assertEquals(id, parentPxzid);

	    	} catch (Exception e) {
	    		System.out.println("error " + e.getClass().toString());
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
	    	}
	    }	
	    
	    private void getParentString(String path){
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
            this.parentPath = p;
	    }
	}
}
