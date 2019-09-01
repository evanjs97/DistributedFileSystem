package cs555.dfs.messaging;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public interface Event {

	public Type getType();

	public byte[] getBytes() throws IOException;

	enum Type {
		CHUNK_WRITE_REQUEST(1),
		CHUNK_READ_REQUEST(2),
		CHUNK_LOCATION_REQUEST(4),
		CHUNK_LOCATION_RESPONSE(8),
		UPLOAD_LOCATION_REQUEST(16),
		UPLOAD_LOCATION_RESPONSE(32);

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
