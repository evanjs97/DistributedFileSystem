package cs555.dfs.util;

import cs555.dfs.messaging.HeartbeatTask;

import java.util.List;

public class Heartbeat implements Comparable<Heartbeat>{
	private final long time;
	private final HeartbeatTask task;


	public Heartbeat(long time, HeartbeatTask task) {
		this.time = time;
		this.task = task;
	}

	@Override
	public int compareTo(Heartbeat h) {
		return Long.compare(h.time, time);
	}

	public long getTime() {
		return this.time;
	}

	public HeartbeatTask getHeartbeatTask() {
		return this.task;
	}
}