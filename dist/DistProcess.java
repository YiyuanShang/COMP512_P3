import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.net.InetAddress;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback {

	ZooKeeper zk;
	String zkServer, pinfo;
	String watcherPath;
	boolean isMaster = false;

	DistProcess(String zkhost) {
		zkServer = zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

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
		zk.create("/dist40/workers", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create("/dist40/tasks", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create("/dist40/unassignedTasks", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
		if (e.getType() == Watcher.Event.EventType.NodeCreated) {
			delegateWorkers();
		}
	};

	Watcher idleWorkersWatcher = (WatchedEvent e) -> {
		if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist40/workers")) {
			delegateWorkers();
		}
	};

	ChildrenCallback idleWorkersCallback = (int rc, String path, Object ctx, List<String> workers) -> {

		if (workers != null && !workers.isEmpty()) {
			for (String worker : workers) {
				try {
					List<String> assignedTasks = zk.getChildren("/dist40/workers/" + worker, taskAssignedWatcher, null);
					if (assignedTasks.isEmpty()) {

						// Worker is idle
						List<String> freeTasks = zk.getChildren("/dist40/unassignedTasks", null);

						String assignableTask = "";
						if (freeTasks.size() != 0) {
							System.out.print("Idle Worker found: " + worker);
							assignableTask = freeTasks.get(0);
							byte[] taskSerial = zk.getData("/dist40/unassignedTasks/" + assignableTask, false, null);
							zk.delete("/dist40/unassignedTasks/" + assignableTask, -1, null, null);
							zk.create("/dist40/workers/" + worker + "/" + assignableTask, taskSerial,
									Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							System.out.println("Worker: " + worker + " was assigned task: " + assignableTask);
						}
					}
					delegateWorkers();
				} catch (NodeExistsException nee) {
					System.out.println(nee);
				} catch (KeeperException ke) {
					System.out.println(ke);
				} catch (InterruptedException ie) {
					System.out.println(ie);
				}
			}

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
		watcherPath = zk.create("/dist40/workers/worker-", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println("DISTAPP: created worker " + watcherPath);
		getTasks(watcherPath);

	}

	/*
	 ****************************************************
	 **************************************************** 
	 * Methods to check for a task to complete.*
	 ****************************************************
	 ****************************************************
	 */

	void getTasks(String path) {
		try {
			List<String> assignedTasks = zk.getChildren(path, assignedTasksWatcher);
			if (!assignedTasks.isEmpty()) {
				String task = assignedTasks.get(0);
				System.out.println("Worker: " + path + " computing task: " + task);
				byte[] taskSerial = zk.getData(path + "/" + task, false, null);

				// Re-construct our task object.
				ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
				ObjectInput in = new ObjectInputStream(bis);
				DistTask dt = (DistTask) in.readObject();

				// Execute the task.
				dt.compute();

				// Serialize our Task object back to a byte array!
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(dt);
				oos.flush();
				taskSerial = bos.toByteArray();

				// Store it inside the result node.
				zk.create("/dist40/tasks/" + task + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				zk.delete(path + "/" + task, -1, null, null);
			}

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
		if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
			getTasks(watcherPath);
		}
	};

	public void process(WatchedEvent e) {
		System.out.println("");
	}

	public void processResult(int rc, String path, Object ctx, List<String> children) {
		System.out.println("");
	}

	public static void main(String args[]) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();
		int count = 0;
		while (true) {
			count++;
		}
	}
}
