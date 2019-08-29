package cs555.dfs.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TCPSender {
	private Socket socket;
	private DataOutputStream dout;

	public TCPSender(Socket socket) throws IOException {
		this.socket = socket;
		dout = new DataOutputStream(socket.getOutputStream());
	}

	public synchronized void sendData(byte[] data) throws IOException {
		int dataLength = data.length;
		dout.writeInt(dataLength);
		dout.write(data, 0, dataLength);

	}

	public synchronized void flush() throws IOException {
		dout.flush();
	}
}