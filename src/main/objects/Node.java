package main.objects;

public class Node {

	private int serverId;
	private int serverPort;
	private int masterPort;
	private int commandPort;
	private int ticks;

	private String serverIp;

	public Node(int myId, int serverPort, int masterPort, int commandPort, String publicIp) {
		this.serverId = myId;
		this.serverPort = serverPort;
		this.masterPort = masterPort;
		this.commandPort = commandPort;
		this.serverIp = publicIp;
		this.ticks = 500;
	}

	public int getServerId() {
		return serverId;
	}

	public int getServerPort() {
		return serverPort;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public int getCommandPort() {
		return commandPort;
	}

	public String getServerIp() {
		return serverIp;
	}
	
	public int getTicks() {
		return ticks;
	}
	
	public String toString() {
		return "myId: " + serverId + " serverPort: " + serverPort + " serverIp: " + serverIp;
	}
}
