package cs555.dfs.util;

import cs555.dfs.messaging.HeartbeatTask;

public class Heartbeat implements Comparable<Heartbeat>{
	private final int time;
	private final HeartbeatTask task;

	Heartbeat(int time, HeartbeatTask task) {
		this.time = time;
		this.task = task;
	}

	@Override
	public int compareTo(Heartbeat h) {
		return Integer.compare(h.time, time);
	}

	public int getTime() {
		return this.time;
	}

	public HeartbeatTask getHeartbeatTask() {
		return this.task;
	}
}