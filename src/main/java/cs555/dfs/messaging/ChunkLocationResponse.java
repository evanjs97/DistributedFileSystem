package cs555.dfs.messaging;

import cs555.dfs.server.ControllerServer;
import cs555.dfs.util.ChunkUtil;

import java.io.*;
import java.util.LinkedList;

public class ChunkLocationResponse implements Event{
	@Override
	public Type getType() {
		return Type.CHUNK_LOCATION_RESPONSE;
	}

	@Override
	public byte[] getBytes() throws IOException {
		byte[] marshalledData;
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(baOutStream));

		dout.writeInt(getType().getValue());

		byte[] chunkBytes;
		dout.writeInt(locations.size());
		for(ChunkUtil chunkServer : locations) {
			chunkBytes = (chunkServer.getHostname() + ":" + chunkServer.getPort()).getBytes();
			dout.writeInt(chunkBytes.length);
			dout.write(chunkBytes);
		}
		dout.flush();
		marshalledData = baOutStream.toByteArray();

		baOutStream.close();
		dout.close();
		return marshalledData;
	}

	private final LinkedList<ChunkUtil> locations;

	public ChunkLocationResponse(LinkedList<ChunkUtil> locations) {
		this.locations = locations;
	}

	public ChunkLocationResponse(DataInputStream din) {
		this.locations = new LinkedList<>();
		try {
			byte[] location;
			int num = din.readInt();
			for (int i = 0; i < num; i++) {
				int length = din.readInt();

				location = new byte[length];
				din.readFully(location);

				String[] split = new String(location).split(":");
				locations.add(new ChunkUtil(split[0], Integer.parseInt(split[1])));

			}
			din.close();

		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public LinkedList<ChunkUtil> getLocations() {
		return locations;
	}
}
