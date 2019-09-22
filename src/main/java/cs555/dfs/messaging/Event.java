package cs555.dfs.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public interface Event {

	Type getType();

	byte[] getBytes() throws IOException;

	enum Type {
		CHUNK_WRITE_REQUEST(0),
		CHUNK_READ_REQUEST(1),
		CHUNK_READ_RESPONSE(2),
		CHUNK_DESTINATION_REQUEST(3),
		CHUNK_DESTINATION_RESPONSE(4),
		CHUNK_LOCATION_REQUEST(5),
		CHUNK_LOCATION_RESPONSE(6),
		REGISTER_REQUEST(7),
		CHUNK_SERVER_HEARTBEAT(8),
		CHUNK_FORWARD_REQUEST(9),
		FILE_LIST_REQUEST(10),
		FILE_LIST_RESPONSE(11);


		private int value;
		private static HashMap<Integer, Type> map = new HashMap<>();

		static {
			for (Type type : Type.values()) {
				map.put(type.value, type);
			}
		}

		public int getValue() {
			return this.value;
		}

		public static Type valueOf(int type) {
			return map.get(type);
		}

		Type(int value) {
			this.value = value;
		}

	}


}
