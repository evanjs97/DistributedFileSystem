package cs555.dfs.server;

import cs555.dfs.messaging.Event;

import java.net.Socket;

public interface Server {
	public void onEvent(Event event, Socket socket);
}
