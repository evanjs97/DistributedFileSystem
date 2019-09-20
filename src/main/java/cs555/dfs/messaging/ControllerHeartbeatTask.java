package cs555.dfs.messaging;

import cs555.dfs.server.ControllerServer;
import cs555.dfs.transport.TCPSender;
import cs555.dfs.util.ChunkUtil;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ControllerHeartbeatTask implements HeartbeatTask{

	private final ControllerServer server;
	private final ConcurrentHashMap<String, TCPSender> senders = new ConcurrentHashMap<>();

	public ControllerHeartbeatTask(ControllerServer server) {
		this.server = server;
	}

	@Override
	public void execute() {
		System.out.println("Starting controller heartbeat");
		List<Map.Entry<String,List<String>>> failedServers = new LinkedList<>();
		for (Map.Entry<String, List<String>> entry : server.getHostToFiles().entrySet()) {
			try {
				String[] destSplit = entry.getKey().split(":");
				Socket socket = new Socket(destSplit[0], Integer.parseInt(destSplit[1]));
				socket.isConnected();
			} catch (IOException e) {
				failedServers.add(entry);
				System.out.println("Found failed server: " + entry.getKey());
			}
		}
		for (Map.Entry<String,List<String>> failure : failedServers) {
			List<String> hostSuccess = server.getHostToFiles().remove(failure.getKey());
			if(hostSuccess == null) {
				System.out.println("Failed to remove host from hostFiles");
			}
				boolean success = server.removeChunkUtil(failure.getKey());
				System.out.println("Removed faulty server: " + failure.getKey() +  success);
		}
		for(Map.Entry<String,List<String>> failure : failedServers) {
			handleFailedServer(failure.getValue());
		}
		closeConnections();
	}

	public void closeConnections() {
		for(TCPSender sender : senders.values()) {
			try {
				sender.close();
			}catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

	private void handleFailedServer(List<String> failedFiles) {
		for(String file : failedFiles) {

			String destination = null;
			for(String dest : server.getFileToServers().get(file)) {
				if(server.getHostToFiles().containsKey(dest)) {
					try {
						String[] splitDest = dest.split(":");
						senders.putIfAbsent(dest, new TCPSender(new Socket(splitDest[0], Integer.parseInt(splitDest[1]))));
						destination = dest;
					}catch(IOException ioe) {
						ioe.printStackTrace();
					}
				}
			}
			if(destination != null) {
				ChunkUtil newLocation = findRandomDestination(file);
				TCPSender sender = senders.get(destination);
				try {
					sender.sendData(new ChunkForwardRequest(file, newLocation.getHostname(), newLocation.getPort()).getBytes());
					sender.flush();
				}catch(IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}

	private ChunkUtil findRandomDestination(String filename) {
		synchronized (server.getChunkServers()) {
			if (server.getChunkServers().isEmpty()) return null;
			ChunkUtil util = server.getChunkServers().pollFirst();
			LinkedList<ChunkUtil> added = new LinkedList<>();
			while (server.getFileToServers().get(filename).contains(util.toString())) {
				added.add(util);
				util = server.getChunkServers().pollFirst();
			}

			TCPSender sender = senders.remove(util.toString());

			server.getHostToServerObject().remove(util.toString());
			util.incrementAssignedChunks();
			server.getHostToServerObject().put(util.toString(), util);
			if(sender != null) senders.put(util.toString(), sender);

			added.add(util);
			server.getChunkServers().addAll(added);

			return util;
		}
	}


}
