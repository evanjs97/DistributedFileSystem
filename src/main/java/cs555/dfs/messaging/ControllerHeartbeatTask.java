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

	private final ConcurrentHashMap<ChunkUtil, List<String>> hostToFiles;
	private final ConcurrentHashMap<String, ControllerServer.SetIndexPair> filesToHost;
	private final ConcurrentHashMap<ChunkUtil, TCPSender> senders = new ConcurrentHashMap<>();
	private ConcurrentSkipListSet<ChunkUtil> chunkServers;

	public ControllerHeartbeatTask(ConcurrentHashMap<ChunkUtil, List<String>> hostToFiles,
								   ConcurrentHashMap<String, ControllerServer.SetIndexPair> filesToHost,
								   ConcurrentSkipListSet<ChunkUtil> chunkServers) {
		this.hostToFiles = hostToFiles;
		this.filesToHost = filesToHost;
		this.chunkServers = chunkServers;
	}

	@Override
	public void execute() {
		System.out.println("Starting controller heartbeat");
		List<Map.Entry<ChunkUtil,List<String>>> failedServers = new LinkedList<>();
		for (Map.Entry<ChunkUtil, List<String>> entry : hostToFiles.entrySet()) {
			try {
				Socket socket = new Socket(entry.getKey().getHostname(), entry.getKey().getPort());
				socket.isConnected();
			} catch (IOException e) {
				failedServers.add(entry);
				System.out.println("Found failed server");
			}
		}
		for (Map.Entry<ChunkUtil,List<String>> failure : failedServers) {
			hostToFiles.remove(failure.getKey());
			chunkServers.remove(failure.getKey());
		}
		for(Map.Entry<ChunkUtil,List<String>> failure : failedServers) {
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
			synchronized (filesToHost) {

				ChunkUtil destination = null;
				for(ChunkUtil dest : filesToHost.get(file).getIndex()) {
					if(hostToFiles.containsKey(dest)) {
						try {
							senders.putIfAbsent(dest, new TCPSender(new Socket(dest.getHostname(), dest.getPort())));
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
	}

	private ChunkUtil findRandomDestination(String filename) {
		System.out.println("TEST");
		synchronized (chunkServers) {
			if(chunkServers.isEmpty()) return null;
			ChunkUtil util = chunkServers.pollFirst();
			LinkedList<ChunkUtil> added = new LinkedList<>();
			while (filesToHost.get(filename).getMap().contains(util) && !chunkServers.isEmpty()) {
				added.add(util);
				util = chunkServers.pollFirst();
			}
			util.incrementAssignedChunks();
			added.add(util);
			chunkServers.addAll(added);

			return util;
		}
	}


}
