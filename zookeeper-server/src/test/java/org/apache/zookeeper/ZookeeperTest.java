package org.apache.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.AddWatchRegistration;
import org.apache.zookeeper.ZooKeeper.DataWatchRegistration;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.cli.CliException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.apache.zookeeper.test.ClientBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test the Zookeeper method "getChildren()"
 */
@RunWith(Enclosed.class)
public class ZookeeperTest {

	@RunWith(Parameterized.class)
	public static class GetChildrenTest extends ClientBase {
		// input variables
		private String path;
		private boolean watcher;
		
		// output variables
		private String expectedOutput;
		
		// variables for testing
		private ZooKeeper zk;
		
		
	    @Before
	    public void setup() throws Exception {
	        zk = createClient();
	        zk.setData("/", "data".getBytes(), -1);
	    	zk.create("/node1", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	        zk.create("/node1/node2", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	        zk.create("/node1/node3", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    }
		
		
		public GetChildrenTest(String path, boolean watcher,String expectedOutput) throws KeeperException, InterruptedException, IOException{
			configure(path, watcher, expectedOutput);
	    }	
	    	
	    public void configure(String path, boolean watcher, String expectedOutput) throws KeeperException, InterruptedException, IOException {
	    	this.path = path;
	    	this.watcher = watcher;
	    	this.expectedOutput = expectedOutput;
	        
	    }
	    
	    @Parameters
	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
                {null, true, "IllegalArgumentException"},
                {"", true, "IllegalArgumentException"},
                {"node1", true, "IllegalArgumentException"},
                {"node1/node2", true, "IllegalArgumentException"},
                {"/node1/node2", true, "0"},
                {"/node1", true, "2"},
                {"/node1", false, "2"},
                {"/node4", true, "KeeperException"},																		
	        });
	    }
	    
	    /// dubbio su true e false del watcher ???
	    
	    @Test(timeout=5000)
	    public void getChildrenTest() {
	        List<String> children = null;
            try {
				children = zk.getChildren(path, watcher);
				assertEquals(Integer.parseInt(expectedOutput), children.size());
			} catch (Exception e) {
	    		System.out.println("error " + e.getClass().toString());
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
			}

	    }
	    
	    @After
	    public void teardown() {
	        if (zk!=null){
	            try {zk.close();}
	             catch (InterruptedException e) {e.printStackTrace();}
	        }
	    }
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	@RunWith(Parameterized.class)
	public static class RemoveWatchesTest extends ClientBase {
		// input variables
		private String path;
		private ClientBase.CountdownWatcher watcher;
		private WatcherType watcherType;
		private boolean local;
	    private int typeTest;
		static int count = 0;
	    
		// output variables
		private String expectedOutput;
		
		// variables for testing
		private ZooKeeper zk;
		
	    @Before
	    public void setup() throws Exception {
	    	zk = createClient();
	        zk.setData("/", "data".getBytes(), -1);
	    	zk.create("/node1", "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    	
	    	if(typeTest == 1) {
	    		if(watcher != null) zk.addWatch("/node1", watcher, AddWatchMode.PERSISTENT);
	    	}
			if(typeTest == 2) {
				    		
			}
			if(typeTest == 3) {
				// Don't add Watcher
			}
			if(typeTest == 4) {
				// add watcher with different type
	    		if(watcher != null) zk.addWatch("/node1", watcher, AddWatchMode.PERSISTENT_RECURSIVE);

			}	    	
	    }
		
		
		public RemoveWatchesTest(String path, ClientBase.CountdownWatcher watcher, WatcherType watcherType, boolean local, int typeTest, String expectedOutput) throws KeeperException, InterruptedException, IOException{
			configure(path, watcher, watcherType, local, typeTest, expectedOutput);
	    }	
	    	
	    public void configure(String path, ClientBase.CountdownWatcher watcher, WatcherType watcherType, boolean local, int typeTest, String expectedOutput) throws KeeperException, InterruptedException, IOException{
	    	this.path = path;
	    	this.watcher = watcher;
	    	this.watcherType = watcherType;
	    	this.local = local;
	    	this.typeTest = typeTest;
	    	this.expectedOutput = expectedOutput;
	    }
	    
	    @Parameters
	    public static Collection<Object[]> getParameters() {
	        return Arrays.asList(new Object[][]{
	            {null, new ClientBase.CountdownWatcher(), WatcherType.Persistent, true, 1, "IllegalArgumentException"},
	            {"node1", new ClientBase.CountdownWatcher(), WatcherType.Persistent, false, 1, "IllegalArgumentException"},
	            {"/node1", null, WatcherType.Persistent, true, 1, "IllegalArgumentException"},
	            {"/node1", new ClientBase.CountdownWatcher(), null, false, 1, "NullPointerException"},
	            {"/node1", new ClientBase.CountdownWatcher(), WatcherType.Persistent, false, 3, "NoWatcherException"},			// No Watcher
	            {"/node1", new ClientBase.CountdownWatcher(), WatcherType.Persistent, true, 4, "NoWatcherException"},				// Watcher present but with different type
	            {"/node1", new ClientBase.CountdownWatcher(), WatcherType.Persistent, false, 1, "0"}             // 
	        });
	    }	    
	   	    
	    @Test(timeout=5000)
	    public void removeWatchesTest() {
	    	System.out.println("\n\n\n" + count +"\n");
	    	count++;
	        System.out.println("starting test");
	        try {
	            zk.removeWatches(path, watcher, watcherType, local);
	            System.out.println(zk.getPersistentWatches());
	            assertEquals(zk.getPersistentWatches().size(),  Integer.parseInt(expectedOutput));
			} catch (Exception e) {
	    		System.out.println("error " + e.getClass().toString());
	            e.printStackTrace();
	    		assertTrue(e.getClass().toString().contains(expectedOutput) && expectedOutput != "");
			}
	    }
	    
	    @After
	    public void teardown() {
	        if (zk!=null){
	            try {zk.close();}
	             catch (InterruptedException e) {e.printStackTrace();}
	        }
	    }
	}
	
}


