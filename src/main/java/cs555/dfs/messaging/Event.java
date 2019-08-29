package cs555.dfs.messaging;

import java.io.IOException;


public interface Event {

	public Type getType();

	public byte[] getBytes() throws IOException;

	enum Type {
		CHUNK_WRITE_REQUEST,
		CHUNK_READ_REQUEST,
		CHUNK_LOCATION_REQUEST,
		CHUNK_LOCATION_RESPONSE,
		UPLOAD_LOCATION_REQUEST,
		UPLOAD_LOCATION_RESPONSE

	}
}
