package cs555.dfs.messaging;

import cs555.dfs.util.ChunkUtil;
import java.io.*;
import java.util.LinkedList;

public class ChunkDestinationResponse implements Event{
	private final LinkedList<ChunkUtil> locations;
	private final String filename;

	@Override
	public Type getType() {
		return Type.CHUNK_DESTINATION_RESPONSE;
	}

	@Override
	public byte[] getBytes() throws IOException {
		MessageMarshaller messageMarshaller = new MessageMarshaller();
		messageMarshaller.writeInt(getType().getValue());
		messageMarshaller.writeChunkUtilList(locations, false);
		messageMarshaller.writeString(filename);
		return messageMarshaller.getMarshalledData();
	}

	public ChunkDestinationResponse(LinkedList<ChunkUtil> locations, String filename) {
		this.filename = filename;
		this.locations = locations;
	}

	public ChunkDestinationResponse(DataInputStream din) {
		this.locations = new LinkedList<>();
		String filename = "";
		try {
			MessageReader messageReader = new MessageReader(din);
			messageReader.readChunkUtilList(locations, false);
			filename = messageReader.readString();
			messageReader.close();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
		this.filename = filename;
	}

	public LinkedList<ChunkUtil> getLocations() {
		return locations;
	}

	public String getFilename() {
		return this.filename;
	}
}
