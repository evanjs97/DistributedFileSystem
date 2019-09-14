package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;
import java.io.*;
import java.util.LinkedList;

public class ChunkDestinationResponse implements Event{
	private final LinkedList<ChunkUtil> locations;

	@Override
	public Type getType() {
		return Type.CHUNK_DESTINATION_RESPONSE;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeChunkUtilList(locations);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkDestinationResponse(LinkedList<ChunkUtil> locations) {
		this.locations = locations;
	}

	public ChunkDestinationResponse(DataInputStream din) {
		this.locations = new LinkedList<>();
		try {
			MessageReader messageReader = new MessageReader(din);
			messageReader.readChunkUtilList(locations);
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public LinkedList<ChunkUtil> getLocations() {
		return locations;
	}
}
