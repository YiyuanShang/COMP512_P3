import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback {
	
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster = false;

	DistProcess(String zkhost) {
		zkServer = zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}
	// public void run(){
	// 	try{
	// 		startProcess();
	// 	}
	// 	catch(Exception e){
	// 		e.printStackTrace();
	// 	}
		
	// }

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
		zk = new ZooKeeper(zkServer, 100000, this); // connect to ZK.
		try {
			runForMaster();
			isMaster = true;
			delegateWorkers();
		} catch (NodeExistsException nee) {
			isMaster = false;
			runForWorker();
		}
		System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));
	}

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to handle master election.*
	 ****************************************************
	 ****************************************************
	 */

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
		zk.create("/dist40/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to assign tasks to workers.*
	 ****************************************************
	 ****************************************************
	 */

	void delegateWorkers() {
		zk.getChildren("/dist40/workers", idleWorkersWatcher, idleWorkersCallback, null);
	}

	Watcher taskAssignedWatcher = (WatchedEvent e) -> {
		System.out.println("DISTAPP : Event received : " + e);

		if (e.getType() == Watcher.Event.EventType.NodeCreated) {
			delegateWorkers();
		}
	};

	Watcher idleWorkersWatcher = (WatchedEvent e) -> {
		System.out.println("DISTAPP : Event received : " + e);

		if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist40/workers")) {
			delegateWorkers();
		}
	};

	ChildrenCallback idleWorkersCallback = (int rc, String path, Object ctx, List<String> workers) -> {
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		for (String worker : workers) {
			System.out.println(worker);
			try {
				List<String> assignedTasks = zk.getChildren("/dist40/workers/" + worker, taskAssignedWatcher, null);
				if (assignedTasks.isEmpty()) {
					List<String> freeTasks = zk.getChildren("/dist40/unassignedTasks", null);
					String assignableTask = freeTasks.get(0);
					byte[] taskSerial = zk.getData("/dist40/unassignedTasks/" + assignableTask, false, null);
					zk.delete("/dist40/unassignedTasks/" + assignableTask, -1, null, null);
					zk.create("/dist40/workers/" + worker + "/" + assignableTask, taskSerial, Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT_SEQUENTIAL);
				}
			} catch (NodeExistsException nee) {
				System.out.println(nee);
			} catch (KeeperException ke) {
				System.out.println(ke);
			} catch (InterruptedException ie) {
				System.out.println(ie);
			}
			//  catch (IOException io) {
			// 	System.out.println(io);
			// } catch (ClassNotFoundException cne) {
			// 	System.out.println(cne);
			// }
		}
	};

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to run for worker.*
	 ****************************************************
	 ****************************************************
	 */
	void runForWorker() throws UnknownHostException, KeeperException, InterruptedException {
		zk.create("/dist40/workers/" + zkServer, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL);
		getTasks(zkServer);
	}

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to check for a task to complete.*
	 ****************************************************
	 ****************************************************
	 */

	void getTasks(String zkServer) {
		try {
			List<String> assignedTasks = zk.getChildren("/dist40/workers/" + zkServer, assignedTasksWatcher);
			String task = assignedTasks.get(0);
			byte[] taskSerial = zk.getData("/dist40/workers/" + zkServer + "/" + task, false, null);

			// Re-construct our task object.
			ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
			ObjectInput in = new ObjectInputStream(bis);
			DistTask dt = (DistTask) in.readObject();

			// Execute the task.
			// TODO: Again, time consuming stuff. Should be done by some other thread and
			// not inside a callback!
			dt.compute();

			// Serialize our Task object back to a byte array!
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(dt);
			oos.flush();
			taskSerial = bos.toByteArray();

			// Store it inside the result node.
			zk.create("/dist40/tasks/" + task + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.delete("/dist40/workers/" + zkServer + "/" + task, -1, null, null);

		} catch (NodeExistsException nee) {
			System.out.println(nee);
		} catch (KeeperException ke) {
			System.out.println(ke);
		} catch (InterruptedException ie) {
			System.out.println(ie);
		} catch (IOException io) {
			System.out.println(io);
		} catch (ClassNotFoundException cne) {
			System.out.println(cne);
		}
	}

	Watcher assignedTasksWatcher = (WatchedEvent e) -> {
		if (e.getType() == Watcher.Event.EventType.NodeCreated) {
			getTasks(zkServer);
		}
	};

	public void process(WatchedEvent e) {
		System.out.println("DISTAPP : Event received : " + e);
	}

	public void processResult(int rc, String path, Object ctx, List<String> children) {
		System.out.println("DISTAPP : Event received : ");
	}
	
	public static void main(String args[]) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		// Replace this with an approach that will make sure that the process is up and
		// running forever.
		Thread.sleep(1000000);
		// Thread t = new Thread(dt);
		// t.start();
		// while(t.isAlive()){
		// 	Thread.sleep(10000);
		// }
		
	}
}
