package cs555.dfs.transport;

import cs555.dfs.util.Heartbeat;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class TCPHeartbeat implements Runnable{


	private final List<Heartbeat> heartbeatIntervals;


	public TCPHeartbeat(List<Heartbeat> heartbeatIntervals) {
		this.heartbeatIntervals = heartbeatIntervals;
		Collections.sort(this.heartbeatIntervals);
	}

	@Override
	public void run() {
		final Instant start = Instant.now();
		long lastSeconds = 0;
		while(true) {
			for(Heartbeat beat : heartbeatIntervals) {
				Instant now = Instant.now();
				Duration duration = Duration.between(start, now);
				//TO DO: add grace period?
				if(duration.getSeconds() % beat.getTime() == 0 && duration.getSeconds() > 0 && lastSeconds != duration.getSeconds()) {
					lastSeconds = duration.getSeconds();
					beat.getHeartbeatTask().execute();
					break;
				}
			}
		}
	}
}
