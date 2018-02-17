package main.peer;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import main.objects.MachineState;
import main.objects.Node;

public class Client extends Thread {

	private Node node;
	private Peer peer;
	private MachineState state;

	private Socket client;
	private DataOutputStream out;
	private DataInputStream in;

	private String msg;
	private String message;
	private boolean closed;
	private boolean active;

	private int pongCounter;

	private Client self;

	public Client(Node node, Peer peer) {
		this.node = node;
		this.peer = peer;

		state = MachineState.DEFAULT;
		self = this;
	}

	public void run() {

		try {
			client = new Socket(node.getServerIp(), node.getServerPort());

			// connection is active
			active = true;

			out = new DataOutputStream(client.getOutputStream());
			in = new DataInputStream(client.getInputStream());

			// start listening server in separate thread
			new Thread(new Runnable() {

				@Override
				public void run() {
					String response = null;

					try {
						// while response line is not null
						while ((response = in.readUTF()) != null) {

							switch (response) {
							case "OK":
								peer.increment();
								break;
							case "ACK":
								peer.incrementAck();
								Log("****ACK from " + node.getServerId() + "****");
								break;
							case "PONG":

								// start election after mutual connection
								if (pongCounter < 1)
									pongCounter++;
								else if (pongCounter == 1) {
									pongCounter++;
									// peer.reset();
									peer.startElection(self);
									// peer.findMaster();
								}

								break;
							case "COMMIT":
								peer.commit();
								break;
							case "ABORT":
								peer.abort();
								break;
							default:
								
								//Log("Replyied with lsn: " + response);
								if(response.startsWith("<lsn>")) {
									//Log("Replyied with lsn 2222: " + response);
									// synchronize transactions with master
									synchronizeWithMaster(response);
								}
								
								break;
							}
						}
					} catch (IOException e) {
						Logerr("***Server " + node.getServerId() + " is down***");

						// start new election if this connection
						// was with the master server
						if (isMaster())
							peer.broadcastElection();

						// find master after election
						// peer.findMaster();

						// close this session
						closed = true;
					}
				}

			}).start();

			while (!closed) {

				switch (state) {
				case DEFAULT:
					out.writeUTF(msg = "PING");
					break;
				case ELECTION:
					peer.reset();
					out.writeUTF(msg = "ELECTION");
					break;
				case MASTER:
					out.writeUTF(msg = "MASTER");
					break;
				case LSN:
					out.writeUTF(msg = "LSN");
					break;
//				case SYNCHRONIZED:
//					out.writeUTF(msg = "SYNCHRONIZED");
//					break;
				case ASK:
					out.writeUTF(msg = message);
					break;
				case PROPOSE:
					out.writeUTF(msg = message);
					break;
				case COMMIT:
					out.writeUTF(msg = "COMMIT");
					break;
				case ABORT:
					out.writeUTF(msg = "ABORT");
					break;
				}
				state = MachineState.DEFAULT;

				// output message
				Log(peer.getMyId() + " sent " + msg + " to " + node.getServerId());

				// Intensity of the communication
				Thread.sleep(node.getTicks());
			}

			Log("Closing client side in " + peer.getMyId());

			client.close();
			peer.removeSession(this);
			clean();
		} catch (InterruptedException e) {
			peer.removeSession(this);
			clean();
		} catch (IOException e) {
			Logerr("*** " + peer.getMyId() + " cannot read, write or reach server " + node.getServerId() + "***");
			peer.removeSession(this);
			clean();
		}
	}
	
	public void synchronizeWithMaster(String lsn) {
		peer.synchronize(lsn, this);
	}
	
	public synchronized void sendMessage(MachineState state) {
		this.state = state;
	}

	public synchronized void sendMessage(MachineState state, String message) {
		this.state = state;
		this.message = message;
	}

	public int getConnectionPort() {
		return node.getServerPort();
	}

	public int getConnectionId() {
		return node.getServerId();
	}

	public Node getConnectedNode() {
		return node;
	}

	public boolean isActive() {
		return active;
	}

	private boolean isMaster() {
		return node.getServerId() == peer.getMasterNode().getServerId();
	}

	private void Log(String message) {
		System.out.println(message);
	}

	private void Logerr(String message) {
		System.err.println(message);
	}

	private void clean() {
		active = false;
		dereference();
	}

	// dereference objects for garbage collection
	private void dereference() {
		this.client = null;
		this.peer = null;
		this.out = null;
		this.in = null;
		this.msg = null;
		// this.node = null;
	}

}
