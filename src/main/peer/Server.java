package main.peer;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import main.objects.Connection;

/**
 * The process is within the thread. But it should have also put its
 * communications also into thread. Server side may get such messages from
 * clients like Ping to which it should respond with Pong, may get Election
 * message to which it should respond with Ok and tell peer side to start
 * election, may get Coordinator message and tell peer side to change
 * coordinator settings
 * 
 * Note: you can use getInetAddress() on socket in order to notify who will
 * become a master node
 */

public class Server extends Thread {

	//private int serverPort;
	private Peer peer;

	// IO from here
	// private DataInputStream in;
	// private DataOutputStream out;

	private ServerSocket serverSocket;
	//private Socket childConnetion;
	
	private boolean isClosed;

	public Server(Peer peer) throws IOException {

		// create server socket
		serverSocket = new ServerSocket(peer.getServerPort());
		
		
		System.out.println("Server started!");

		//this.serverPort = serverPort;
		this.peer = peer;
	}

	public void run() {
		
		// start server side in a while loop in order to receive
		// connections from client side peers

		while (true) {

			Socket childConnetion = null;

			try {
				childConnetion = serverSocket.accept();
				
				System.out.println("Child connected!");

				
				// start connection here
				Connection connection = new Connection(childConnetion, peer);
				connection.start();
				
				// check for active ports every minute
				// reopen sessions every minute
				/*new Thread(new Runnable() {

					@Override
					public void run() {
						// TODO Auto-generated method stub
						while(!isClosed) {
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							peer.statistics();
							//peer.reopenSessions();
							//peer.fire();
						}
					}
					
				}).start();*/

			} catch (SocketTimeoutException e) {
				//peer.shutDown();
				System.err.println("Timeout on the client " + childConnetion.getInetAddress());
				break;
				// e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Cannot read, write or connect to the client " + childConnetion.getInetAddress());
				break;
				// e.printStackTrace();
			} catch (Exception e) {
				System.err.println(
						"Some general exception while connecting to the client " + childConnetion.getInetAddress());
				break;
			}

		}
	}
}


