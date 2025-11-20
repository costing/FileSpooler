package metacreator;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ListingThread implements Runnable {
	private BlockingQueue<String> dirs;
	private BlockingQueue<XrootdFile> files;

	private static Logger logger = ConfigUtils.getLogger(ListingThread.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(ListingThread.class.getCanonicalName());

	ListingThread(BlockingQueue<String> dirs, BlockingQueue<XrootdFile> files) {
		this.dirs = dirs;
		this.files = files;
	}

	@Override
	public void run() {
		while (!dirs.isEmpty()) {
			try {
				String path = dirs.take();
				logger.log(Level.INFO, "Listing dir: " + path);
				try (Timing t = new Timing(monitor, "listing_execution_time")) {
					addFiles(path);
				}
			}
			catch (InterruptedException | IOException e) {
				logger.log(Level.WARNING, "Caught exception in listing thread!", e);
			}
		}
	}

	private void addFiles(String path) throws IOException {
		XrootdListing listing = new XrootdListing(analyzer.Main.getXrootdClient(), path);
		Set<XrootdFile> directories = listing.getDirs();
		Set<XrootdFile> listFiles = listing.getFiles();

		for (XrootdFile file : listFiles) {
			files.add(file);
			Main.nrFilesProcessed.getAndIncrement();
		}

		for (XrootdFile dir : directories) {
			addFiles(dir.getFullPath());
		}
	}
}
