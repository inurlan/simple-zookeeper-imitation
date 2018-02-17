package main.objects;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

import main.peer.Peer;

public class Connection extends Thread {

	// IO from here
	private DataInputStream in;
	private DataOutputStream out;
	Socket childConnetion;

	private String childIp;

	private Peer peer;

	private int pingCounter;

	private ArrayList<Connection> connections;

	public Connection(Socket childConnetion, Peer peer) throws IOException {
		this.childConnetion = childConnetion;
		this.childIp = childConnetion.getInetAddress().getHostAddress();
		this.peer = peer;
	}

	@Override
	public void run() {

		// some initializations
		String message;

		try {
			in = new DataInputStream(childConnetion.getInputStream());
			out = new DataOutputStream(childConnetion.getOutputStream());
		} catch (IOException e) {
			Logerr("Cannot read, write or connect to the client " + childConnetion.getInetAddress());
		}

		try {
			// read messages from connected client here
			while ((message = in.readUTF()) != null) {

				/**/
				Log(peer.getMyId() + " got message: " + message + " from " + childIp);
				/**/

				switch (message) {
				case "PING":
					out.writeUTF("PONG");

					// open mutual connection after 2nd ping
					if (pingCounter < 1)
						pingCounter++;
					else if (pingCounter == 1) {
						peer.reopenSession(childIp);
						pingCounter++;
					}

					break;
				case "ELECTION":
					// peer.reopenSessions();
					out.writeUTF("OK");
					peer.broadcastElection();
					peer.findMaster();
					break;
				case "MASTER":
					peer.setMaster(childConnetion);
					out.writeUTF("Ok");
					break;
//				case "SYNCHRONIZED":
//					out.writeUTF("Ok");
//					peer.broadcastElection();
//					peer.findMaster();
//					break;
				case "COMMIT":
					peer.commit();
					out.writeUTF("Ok");
					break;
				case "ABORT":
					peer.abort();
					out.writeUTF("Ok");
				case "LSN":
					out.writeUTF(peer.replyLsn());
					break;
				default:

					if (message.startsWith("<propose>")) {
						action(MachineState.PROPOSE, message);
					} else if (message.startsWith("<lsn>")) {
						out.writeUTF("Ok");
						peer.replyPropose(childIp, message);
					}
					break;
				}
			}

			// some clean up
			in.close();
			out.close();
			childConnetion.close();
		} catch (IOException e) {
			Logerr("Cannot read from the " + childIp);
		}

	}
	
	/// remove state part 

	private /*synchronized*/ void action(MachineState machineState, String propose) {
		
		try {
			
			// if this is master
			if (peer.getMyId() == peer.getMasterNode().getServerId()) {
				out.writeUTF("ACK");
				peer.action(machineState, propose);
			} else {
				peer.save(propose);
				out.writeUTF("ACK");
			}
			
		} catch (IOException e) {
			Logerr("Cannot raed from the " + childIp);

		}
		

	}

	private void Log(String message) {
		System.out.println(message);
	}

	private void Logerr(String message) {
		System.err.println(message);
	}

}