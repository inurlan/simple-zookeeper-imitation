package main;

import main.objects.FileIO;
import main.objects.Node;
import main.peer.Peer;

public class Start {
	
	/*
	 * 
	 * <servers>
	 * 		<server><id>1</id><sport>3555</sport><cport><5555/cport><ip>17.27.15.16</ip></server>
	 * </servers>
	 * 
	 * */
	
	//private static FileIO settings;

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		
		// open settings file in order to 
		//settings = new FileIO("settings");
		

		String[] hosts = new String[args.length-1];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = args[i].trim();
		}
		
		startFromTerminal(hosts, args[hosts.length]);
		
		 //startFromConsole();

	}

	public static void startFromTerminal(String[] hosts, String arg) {
		Node node1 = new Node(1, 3555, 4555, 5555, hosts[0]);
		Node node2 = new Node(2, 3555, 4555, 5555, hosts[1]);
		Node node3 = new Node(3, 3555, 4555, 5555, hosts[2]);
		Node node4 = new Node(4, 3555, 4555, 5555, hosts[3]);

		Node[] nodes = { node1, node2 , node3, node4};
		Node[] subnodes = new Node[nodes.length - 1];
		Node node = null;

		for (int i = 0, j = 0; i < nodes.length; i++) {
			if (i + 1 == (int) new Integer(arg)) {
				node = nodes[i];
			} else {
				subnodes[j++] = nodes[i];
			}
		}

		// debug
		// System.out.println(node.toString());
		//
		// System.out.println("Sub nodes: ");
		//
		// for (int i = 0; i < subnodes.length; i++) {
		// System.out.println(subnodes[i].toString());
		// }

		Peer peer = new Peer(node, subnodes);
		peer.start();
	}

	public static void startFromConsole() throws InterruptedException {
		Node node1 = new Node(1, 3555, 4555, 5555, "127.0.0.1");
		Node node2 = new Node(2, 3556, 4555, 5555, "127.0.0.1");
		Node node3 = new Node(3, 3557, 4555, 5555, "127.0.0.1");
		Node node4 = new Node(4, 3558, 4555, 5555, "127.0.0.1");

		Node[] nodes2 = { node1, node2, node4 };
		Peer peer2 = new Peer(node3, nodes2);

		// Thread.sleep(5000);

		peer2.start();

		Node[] nodes = { node1, node2, node3 };

		Peer peer = new Peer(node4, nodes);

		Thread.sleep(5000);
		peer.start();

		Node[] nodes4 = { node2, node3, node4 };
		Peer peer4 = new Peer(node1, nodes4);

		Thread.sleep(5000);

		peer4.start();

		Node[] nodes3 = { node1, node3, node4 };
		Peer peer3 = new Peer(node2, nodes3);

		Thread.sleep(10000);

		peer3.start();
	}

}
