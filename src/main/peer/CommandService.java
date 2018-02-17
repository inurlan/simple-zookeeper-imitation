package main.peer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import main.objects.FileIO;

public class CommandService extends Thread {

	private Peer peer;

	private ServerSocket serverSocket;

	private DataInputStream in;
	private DataOutputStream out;

	private boolean closed;

	public CommandService(Peer peer) throws IOException {
		serverSocket = new ServerSocket(peer.getCommandPort());

		this.peer = peer;
	}

	public void run() {
		// start server side in a while loop in order to receive
		// connections from client side peers

		while (true) {

			Socket commandConnetion = null;

			try {
				commandConnetion = serverSocket.accept();
				Log("IP " + commandConnetion.getInetAddress().getHostAddress() + " connected!");

				in = new DataInputStream(commandConnetion.getInputStream());
				out = new DataOutputStream(commandConnetion.getOutputStream());

				// get
				new Thread(new Runnable() {

					@Override
					public void run() {
						String message;

						try {

							while ((message = in.readUTF()) != null) {
								/**/
								Log(peer.getMyId() + " got message: " + message);
								/**/
								String option = FileIO.extract("propose", message, "inner");
										
								switch (option) {
								case "create":
									peer.create(message);
									out.writeUTF("OK");
									break;
								case "delete":
									peer.delete(message);
									out.writeUTF("OK");
									break;
								case "append":
									peer.append(message);
									out.writeUTF("OK");
									break;
								case "read":
									peer.read(message);
									break;
								}

							}
						} catch (IOException e) {
							Logerr("***Commander disconnected***");
						}

					}
				}).start();

			} catch (Exception e) {
				Logerr("***Some Error Occured!***");
			}
		}

	}

	public void respondMessage(String message) {
		try {
			out.writeUTF(message);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void Log(String message) {
		System.out.println(message);
	}

	private void Logerr(String message) {
		System.err.println(message);
	}

}
