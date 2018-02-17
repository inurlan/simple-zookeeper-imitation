package main.peer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import main.objects.FileIO;
import main.objects.MachineState;
import main.objects.Node;

public class Peer extends Thread {

	/**
	 * First step is to implement leader election algorithm where each node should
	 * have peer.getServerId(), peer-peer port, master-server port and port for
	 * client connection
	 * 
	 * In order to implement leader election algorithm, each peer should hold a
	 * unique ID. peer-peer port is Server side port for peer to peer communication
	 * among nodes. Once master node is elected it opens up master-server port. This
	 * port will be used mainly for file sharing and etc. Client connection port is
	 * used for listening outside connections for retrieving commands of files. Also
	 * peer node should hold its own IP
	 */

	// info about this peer
	private Node peer;

	// info about master peer
	private Node masterNode;

	// info about other nodes
	private Node[] nodes;

	// reference to connected sessions
	private ArrayList<Client> sessions;

	// server side of this peer
	private Server server;

	private CommandService commandService;

	// number of OK answers
	private int answers;

	// number if ACKs for two-phase commit
	private int ackNumbers;

	// laster move this to start
	private FileIO log;

	// later move this to start
	private FileIO dirtyPage;

	// constructor accepts this node and list of peer nodes
	public Peer(Node node, Node[] nodes) {

		// peer initially becomes masterNode
		peer = masterNode = node;

		// init list of peer nodes
		this.nodes = nodes;

		// inti sessions array list
		sessions = new ArrayList<Client>();
	}

	public void run() {

		// Start own server side process

		try {
			// start server side peer
			server = new Server(this);
			server.start();

			// open sessions with other peers for communication
			openSessions();

			// start commandService for accepting transactions from Commander client
			commandService = new CommandService(this);
			commandService.start();
		} catch (IOException e) {
			Logerr("Problem");
		}

		// creates log and dirtyPage files
		log = new FileIO("data/log");
		dirtyPage = new FileIO("data/dirty_page");
	}

	// opens sessions among multiple clients and servers
	public void openSessions() {
		Client temp = null;
		for (Node node : nodes) {
			sessions.add(temp = new Client(node, this));
			temp.start();
		}
	}

	// opens session between single client and server
	public boolean openSession(Client client) {

		Client temp = null;

		for (Client session : sessions) {
			if (session.getConnectionId() == client.getConnectionId()) {
				return false;
			}
		}

		sessions.add(temp = client);
		temp.start();

//		// send that he is master if true
//		if (peer.getServerId() == masterNode.getServerId())
//			temp.sendMessage(MachineState.MASTER);

		return true;
	}

	public synchronized void startElection(Client session) {
		if (session.getConnectionId() > peer.getServerId()) {
			session.sendMessage(MachineState.ELECTION);
			// smallId = true;
		}
	}

	// broadcasts election to all servers who have higher id
	public synchronized void broadcastElection() {
		reset();

		for (Client session : sessions) {
			if (session.getConnectionId() > peer.getServerId()) {
				session.sendMessage(MachineState.ELECTION);
			}
		}
	}

	public /* synchronized */ void reopenSession(String connectionIp) {
		for (Node node : nodes) {
			if (node.getServerIp().equals(connectionIp)) {
				if (openSession(new Client(node, this))) {
					Log(peer.getServerId() + " reopen Session! port: " + node.getServerPort() + " serverID: "
							+ node.getServerId());
				}

			}
		}
	}

	public /* synchronized */ void findMaster() {
		// timeout of OK answers
		try {
			Thread.sleep(1600);

			synchronized (this) {
				// Log(peer.getServerId() + " has number of OKS: " + get());

				if (get() == 0) {
					broadcast(MachineState.MASTER);
				}
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}

	}

	public void verify() {
		// timeout of OK answers

		if (sessions.size() > 0) {
			try {
				Thread.sleep(1600);

				// Log(peer.getServerId() + " has number of OKS: " + get());

				Log("ACKS: " + getAck() + " SIZE: " + sessions.size());
				if (getAck() == sessions.size()) {
					broadcast(MachineState.COMMIT);
					// commit to log file the changes in master node
					commit();
				} else {
					broadcast(MachineState.ABORT);

					// make clean up in master node
					// clean dirty page
					abort();
				}

			} catch (InterruptedException e) {
				Logerr("Some error happend");
			}
		} else {
			// commit to log file the changes in master node
			commit();
		}
	}

	public /* synchronized */ void broadcast(MachineState machineState) {

		if (machineState.equals(MachineState.MASTER))
			masterNode = peer;

		for (Client session : sessions) {
			session.sendMessage(machineState);
		}
	}

	public /* synchronized */ void broadcast(MachineState machineState, String message) {

		for (Client session : sessions) {
			session.sendMessage(machineState, message);
		}
	}

	public void setMaster(Node node) {
		masterNode = node;
	}

	public void setMaster(Socket childConnetion) {
		for (Client session : sessions) {
			if (childConnetion.getInetAddress().getHostAddress().equals(session.getConnectedNode().getServerIp())) {
				this.masterNode = session.getConnectedNode();

				// start synchronization process with the master
				session.sendMessage(MachineState.LSN);
			}
		}

		if (masterNode != null)
			Logerr(peer.getServerId() + " says that master is " + masterNode.toString());
	}

	// save propose to the dirty page
	public /* synchronized */ void save(String propose) {
		try {

			dirtyPage.addTransaction(propose);

			Log("****** " + peer.getServerId() + " SAVED TO THE DIRTY PAGE!*******");
		} catch (IOException e) {
			Logerr("****** " + peer.getServerId() + " FAILED TO SAVE TO THE DIRTY PAGE!*******");
		}
	}

	// perform commit action and save to log page
	public /* synchronized */ void commit() {
		String propose = "";

		try {
			log.addTransaction(propose = FileIO.extract("lsn", dirtyPage.findLastTransaction(), "outer"));
			Log("****** " + peer.getServerId() + " COMMITED TO LOG!*******");
		} catch (IOException e) {
			Logerr("****** " + peer.getServerId() + " FAILED TO COMMIT TO LOG!*******");
		}

		if (propose.startsWith("<propose>")) {

			String option = FileIO.extract("propose", propose, "inner");

			propose = FileIO.extract("propose", propose, "outer");

			switch (option) {
			case "create":
				executeCreation(propose);
				break;
			case "delete":
				executeDelete(propose);
				break;
			case "append":
				executeAppend(propose);
				break;
			}
		}

		// if this is not master
		if (peer.getServerId() != masterNode.getServerId()) {
			for (Client session : sessions) {
				// if this session is between master and this node
				if (session.getConnectionId() == masterNode.getServerId()) {
					session.sendMessage(MachineState.LSN);
				}
			}
		}
	}

	// synchronize node state with master
	public void synchronize(String lsn, Client session) {

		Integer lastLsn = Integer.parseInt(FileIO.extract("lsn", lsn, "inner"));

		if (lastLsn > log.lastLsn()) {
			// ask transaction lastLsn + 1
			session.sendMessage(MachineState.ASK, "<lsn>" + (log.lastLsn() + 1) + "</lsn>");
		}/* else if (peer.getServerId() > masterNode.getServerId()) {
			broadcast(MachineState.SYNCHRONIZED);
		}*/
	}

	public String replyLsn() {
		return "<lsn>" + log.lastLsn() + "</lsn>";
	}

	public void replyPropose(String connectionIp, String lsn) {

		Integer askedLsn = Integer.parseInt(FileIO.extract("lsn", lsn, "inner"));

		resetAck();

		for (Client session : sessions) {
			// find master session
			if (session.getConnectedNode().getServerIp().equals(connectionIp)) {
				try {

					// extract propose from transaction and send it to the node
					session.sendMessage(MachineState.PROPOSE,
							FileIO.extract("lsn", log.findTransaction(askedLsn), "outer"));

					verifySingle(session);
				} catch (IOException e) {
					Logerr("****** " + peer.getServerId() + " FAILED TO FIND TRANSACTION!*******");
				}
			}
		}
	}

	public void verifySingle(Client session) {
		// timeout of OK answers

		try {
			Thread.sleep(1100);
			Log("ACKS: " + getAck() + " SIZE: " + sessions.size());

			if (getAck() == 1) {
				session.sendMessage(MachineState.COMMIT);
			} else {
				session.sendMessage(MachineState.ABORT);
			}
		} catch (InterruptedException e) {
			Logerr("Some error happend");
		}
	}

	// clean dirt page
	public /* synchronized */ void abort() {

		try {
			dirtyPage.deleteLastTransaction();
			Log("****** " + peer.getServerId() + " ABORTED FROM THE DURTY PAGE!*******");
		} catch (IOException e) {
			Logerr("****** " + peer.getServerId() + " FAILED TO ABORT FROM THE DURTY PAGE!*******");
		}
	}

	// creation of file with specified file path and data if exists delete first
	public /* synchronized */ void executeCreation(String propose) {

		String filePath = FileIO.extract("path", propose, "inner");
		String data = FileIO.extract("data", FileIO.extract("path", propose, "outer"), "inner");

		// delete old file if exist
		executeDelete(propose);

		FileIO newFile = new FileIO(filePath);

		try {
			newFile.add(data);
			newFile.close();
			Log("****** " + peer.getServerId() + " CREATED FILE AND ADDED LINE SUCCESSFULLY!*******");
		} catch (IOException e) {
			Logerr("****** " + peer.getServerId() + " FAILED TO CREATE AND ADD LINE!*******");
		}
	}

	// delete file
	public /* synchronized */ void executeDelete(String propose) {

		String filePath = FileIO.extract("path", propose, "inner");

		if (FileIO.exists(filePath)) {
			try {
				FileIO.deleteFile(filePath);
				Log("****** " + peer.getServerId() + " SUCCESSFULLY DELETED!*******");
			} catch (IOException e) {
				Logerr("****** " + peer.getServerId() + " FAILED TO DELETED!*******");
			}
		} else

			Logerr("****** " + peer.getServerId() + " NO SUCH FILE!*******");
	}

	// append data to existed file
	public /* synchronized */ void executeAppend(String propose) {

		String filePath = FileIO.extract("path", propose, "inner");
		String data = FileIO.extract("data", FileIO.extract("path", propose, "outer"), "inner");

		if (FileIO.exists(filePath)) {
			FileIO newFile = new FileIO(filePath);

			try {
				newFile.add(data);
				newFile.close();
				Log("****** " + peer.getServerId() + " APPENDED LINE SUCCESSFULLY!*******");
			} catch (IOException e) {
				Logerr("****** " + peer.getServerId() + " FAILED TO APPEND LINE!*******");
			}
		} else

			Logerr("****** " + peer.getServerId() + " NO SUCH FILE!*******");
	}

	public /* synchronized */ void executeRead(String propose) {

		String filePath = FileIO.extract("path", propose, "inner");

		if (FileIO.exists(filePath)) {

			FileIO newFile = new FileIO(filePath);

			String data;

			try {
				data = newFile.readAll();
				commandService.respondMessage(data);
				Log("****** " + peer.getServerId() + " READ LINE SUCCESSFULLY!*******");
			} catch (IOException e) {
				Logerr("****** " + peer.getServerId() + " FAILED TO READ LINE!*******");
			}
		} else

			Logerr("****** " + peer.getServerId() + " NO SUCH FILE!*******");
	}

	public void create(String propose) {
		action(MachineState.PROPOSE, propose);
	}

	public void append(String propose) {
		action(MachineState.PROPOSE, propose);
	}

	public void delete(String propose) {
		action(MachineState.PROPOSE, propose);
	}

	// read operation is not required to perform from the master node
	public void read(String propose) {
		propose = FileIO.extract("propose", propose, "outer");
		executeRead(propose);
	}

	public /* synchronized */ void action(MachineState machineState, String propose) {

		// delete this log
		Log(peer.getServerId() + "got message: " + propose);

		// if this is master node
		if (peer.getServerId() == masterNode.getServerId()) {

			// add propose to the dirty page
			save(propose);

			// reset ACK before broadcasting it to be sure that counting starts from 0
			resetAck();

			// broadcast propose to everyone
			for (Client session : sessions) {
				session.sendMessage(machineState, propose);
			}

			// verify that transaction in other nodes' dirty pages and send commit
			verify();

		} else {

			// send propose to the master node
			for (int index = sessions.size() - 1; index >= 0; index--) {

				// if this sessions opened for master node
				if (sessions.get(index).getConnectedNode().getServerId() == masterNode.getServerId()) {

					// send propose to the master node
					sessions.get(index).sendMessage(machineState, propose);
					break;
				}
			}
		}
	}

	public void statistics() {
		Logerr(peer.getServerId() + " sessions array size is: " + sessions.size());
		Logerr("***Master node for " + peer.getServerId() + " is " + masterNode.getServerId() + "***");
	}

	public synchronized void increment() {
		answers++;
		Log(peer.getServerId() + " incremented and values is " + answers);
	}

	public synchronized void decrement() {
		answers++;
		Log(peer.getServerId() + " decremented and values is " + answers);

	}

	public synchronized void reset() {
		answers = 0;
		Log(peer.getServerId() + " reset and values is " + answers);

	}

	public synchronized int get() {
		Log(peer.getServerId() + " get and values is " + answers);
		return answers;
	}

	public synchronized void incrementAck() {
		ackNumbers++;
		// Logerr(peer.getServerId() + " incremented and values is " + ackNumbers);

		// if (ackNumbers == sessions.size()) {
		// broadcast(MachineState.COMMIT);
		// ackNumbers = 0;
		// }
	}

	public synchronized void decrementAck() {
		ackNumbers++;
		// Log(peer.getServerId() + " decremented and values is " + ackNumbers);
	}

	public synchronized void resetAck() {
		ackNumbers = 0;
		// Log(peer.getServerId() + " reset and values is " + ackNumbers);

	}

	public synchronized int getAck() {
		// Log(peer.getServerId() + " get and values is " + ackNumbers);
		return ackNumbers;
	}

	public void removeSession(Client client) {
		sessions.remove(client);
	}

	public int getMyId() {
		return peer.getServerId();
	}

	public Node[] getNodes() {
		return nodes;
	}

	public int getServerPort() {
		return peer.getServerPort();
	}

	public int getMasterPort() {
		return masterNode.getMasterPort();
	}

	public int getCommandPort() {
		return peer.getCommandPort();
	}

	public String getPublicIp() {
		return peer.getServerIp();
	}

	public int getCoordinatorId() {
		return masterNode.getServerId();
	}

	public Node getMasterNode() {
		return masterNode;
	}

	public Node getMyNode() {
		return peer;
	}

	public synchronized ArrayList<Client> sessions() {
		return sessions;
	}

	private void Log(String message) {
		System.out.println(message);
	}

	private void Logerr(String message) {
		System.err.println(message);
	}

}
