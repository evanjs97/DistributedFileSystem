package cs555.dfs.transport;

import cs555.dfs.util.Heartbeat;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class TCPHeartbeat implements Runnable{


	private final List<Heartbeat> heartbeatIntervals;
	private final String destHost;
	private final int destPort;

	public TCPHeartbeat(List<Heartbeat> heartbeatIntervals, String destHost, int destPort) {
		this.heartbeatIntervals = heartbeatIntervals;
		Collections.sort(this.heartbeatIntervals);
		this.destHost = destHost;
		this.destPort = destPort;
	}

	@Override
	public void run() {
		final Instant start = Instant.now();
		while(true) {
			for(Heartbeat beat : heartbeatIntervals) {
				Instant now = Instant.now();
				Duration duration = Duration.between(start, now);
				//TO DO: add grace period?
				if(duration.getSeconds() == beat.getTime()) {
					beat.getHeartbeatTask().execute();
					break;
				}
			}
		}
	}
}
