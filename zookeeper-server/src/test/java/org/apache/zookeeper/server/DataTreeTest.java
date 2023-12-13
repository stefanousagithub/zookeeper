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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.*;

@RunWith(Enclosed.class)
public class DataTreeTest {
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
                this.dataTree.createNode(currentPath, new byte[100], null, isFatherEph, dataTree.getNode(nodeParent).stat.getCversion(), 0, 1);
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
	    
//    } else if (testType == 7) {
//        this.dtQuotas = new DataTree();
//        this.dtQuotas.createNode(Quotas.quotaZookeeper+"/parent", new byte[0], null, 0, 0,1, 1);
//    }else if (testType == 8) {
//        this.dtStat = new DataTree();
//        this.dtStat.createNode("/zn1", new byte[0], null, 0, 0,1, 1, new Stat());
//    }else if (testType == 9) {
//        this.dtQuotas = new DataTree();
//        this.dtQuotas.createNode("/parent", new byte[0], null, 0, 0,1, 1);
//    }
	    
//	    /zookeeper/quota/node1/zookeeper_limits
	    
	    
	
	    @Parameters
	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, -1, -1, -1, -1, 1, ""},										// 0
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0, 0, 0, 0, 1, ""},											// 1
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xff00000000000001L, 1, 2, 1, 1, ""},								// 2
                {"/node1", 10, ZooDefs.Ids.CREATOR_ALL_ACL, 0x8000000000000000L, 5, 3, 1000, 1, ""},								// 3
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
               
                {"/node1", 10, ZooDefs.Ids.READ_ACL_UNSAFE, 0xff00000000000001L, -1, 2, -1, 5, ""},					     // 19

                // add quora
                {"/node1"+"/"+Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},											// 
                {Quotas.statNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 7, ""},											// 
                {"/node1"+"/"+Quotas.limitNode + "/node2", 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},
                {"/node1"+"/"+ Quotas.limitNode + "/"+ Quotas.limitNode, 1000, ZooDefs.Ids.CREATOR_ALL_ACL, 1, 0, 1, 1, 6, ""},											// 
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
		        assertArrayEquals(data, createdNode.getData());
	    	} catch (Exception e) {
	    		System.out.println("error " + e.getClass().toString());
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
	    	}
	    }	       
	   
	}
}
