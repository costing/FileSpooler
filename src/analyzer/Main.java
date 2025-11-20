package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.client.XrootdClient;
import alien.se.SE;
import lazyj.ExtProperties;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
	static ExtProperties listingProperties;
	private static final String defaultSEName = "ALICE::CERN::EOSALICEO2";
	static final String defaultseioDaemons = "root://eosaliceo2.cern.ch:1094";
	private static final String defaultListingDirs = "/data/epn2eos_tool/listingDirs";
	private static final int defaultThreads = 4;
	private static BlockingQueue<String> dirs = new LinkedBlockingDeque<>();

	static SE se = null;

	private static Logger logger = ConfigUtils.getLogger(Main.class.getCanonicalName());

	public static XrootdClient getXrootdClient() {
		try {
			return se.getXrootdClient();
		}
		catch (Exception e) {
			logger.log(Level.SEVERE, "Could not get an XrootdClient instance for " + se, e);
		}
		
		return null;
	}

	public static void main(String[] args) throws IOException {
		File listingDirs;
		listingProperties = ConfigUtils.getConfiguration("listing");

		if (listingProperties == null) {
			logger.log(Level.WARNING, "Cannot find listing config file");
			return;
		}

		logger.log(Level.INFO, "Storage Element Name: " + listingProperties.gets("seName", defaultSEName));
		logger.log(Level.INFO, "Storage Element seioDaemons: " + listingProperties.gets("seioDaemons", defaultseioDaemons));
		logger.log(Level.INFO, "Listing Dirs Path: " + listingProperties.gets("listingDirs", defaultListingDirs));
		logger.log(Level.INFO, "Number of listing threads: " + listingProperties.geti("queue.default.threads", defaultThreads));

		listingDirs = new File(listingProperties.gets("listingDirs", defaultListingDirs));
		if (!listingDirs.exists() || listingDirs.length() == 0) {
			logger.log(Level.WARNING, "The file that contains the list of dirs to be processed does not exist or is empty");
			return;
		}

		se = new SE(listingProperties.gets("seName", defaultSEName), 1, "special", "/", listingProperties.gets("seioDaemons", defaultseioDaemons));

		Set<String> scandDirs = ListingUtils.getScanDirs(listingProperties.gets("listingDirs", defaultListingDirs));
		for (String scan : scandDirs) {
			Set<XrootdFile> firstLevelDirs = ListingUtils.getFirstLevelDirs(scan);
			for (XrootdFile dir : firstLevelDirs) {
				logger.log(Level.INFO, "Dir path: " + dir.getFullPath());
				dirs.add(dir.getFullPath());
			}

		}

		Thread[] threads = new Thread[listingProperties.geti("queue.default.threads", defaultThreads)];
		for (int i = 0; i < listingProperties.geti("queue.default.threads", defaultThreads); i++) {
			threads[i] = new Thread(new ListingThread(dirs));
			threads[i].start();
		}
	}
}
