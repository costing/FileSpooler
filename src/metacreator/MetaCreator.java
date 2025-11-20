package metacreator;

import alien.catalogue.*;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Xrootd;
import alien.io.xrootd.XrootdFile;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SE;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MetaCreator implements Runnable {
	private BlockingQueue<XrootdFile> files;
	private static Logger logger = ConfigUtils.getLogger(MetaCreator.class.getCanonicalName());
	private static final Monitor monitor = MonitorFactory.getMonitor(MetaCreator.class.getCanonicalName());
	private static final String URL = "http://alimonitor.cern.ch/epn2eos/lfncheck.jsp";

	MetaCreator(BlockingQueue<XrootdFile> files) {
		this.files = files;
	}

	@Override
	public void run() {
		while (true) {
			try {
				XrootdFile file = files.take();
				logger.log(Level.INFO, "Metacreator thread processes the file: " + file.getFullPath());
				createMetadataFile(file);
				Main.nrFilesProcessed.getAndDecrement();
			}
			catch (InterruptedException e) {
				logger.log(Level.WARNING, "MetaCreator thread was interrupted while waiting", e);
			}
		}
	}

	private String encode(final String s) {
		return URLEncoder.encode(s, StandardCharsets.UTF_8);
	}

	private boolean sendRequest(String curl) {
		int status;

		try {
			java.net.URL url = new URL(URL);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoOutput(true);
			connection.setConnectTimeout(1000 * 10);
			connection.setReadTimeout(1000 * 60 * 2);

			String urlParam = encode("curl") + "=" + encode(curl);

			try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream())) {
				writer.write(urlParam);
			}

			status = connection.getResponseCode();
			connection.disconnect();
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Communication error", e.getMessage());
			status = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
		}

		return status == HttpServletResponse.SC_FOUND;
	}

	private void createMetadataFile(XrootdFile file) {
		String seName, seioDaemons, surl, curl, metaFileName, extension, tmpFile, md5Output, srcPath, destPath,
				run, period, lurl;
		UUID guid;
		long size, ctime;

		metaFileName = "meta-" + file.getFullPath().replace("/eos/aliceo2/ls2data/", "")
				.replace('/', '-') + ".done";

		srcPath = Main.metacreatorProperties.gets("metaDir", Main.defaultMetadataDir) + "/" + metaFileName;
		destPath = Main.metacreatorProperties.gets("registrationDir", Main.defaultRegistrationDir) + "/"
				+ metaFileName;

		run = "499999";
		period = "LHC21z";
		lurl = destPath;
		if (file.getFullPath().contains("run")) {
			for (String s : file.getFullPath().split("/")) {
				if (s.contains("_")) {
					if (s.contains("run0")) {
						run = s.substring(4, s.indexOf('_'));
						break;
					}
					else
						if (s.contains("run")) {
							run = s.substring(3, s.indexOf('_'));
							break;
						}
				}
			}
		}

		seName = Main.metacreatorProperties.gets("seName", Main.defaultSEName);
		seioDaemons = Main.metacreatorProperties.gets("seioDaemons", Main.defaultseioDaemons);
		surl = file.getFullPath();
		curl = "/alice/data/2021/" + period + "/" + run + "/" + file.getFullPath().replace("/eos/aliceo2/ls2data/", "");
		guid = GUIDUtils.generateTimeUUID();
		size = file.getSize();
		ctime = file.getmTime().getTime();

		tmpFile = "/data/epn2eos_tool/tmp_file_" + guid;

		if (sendRequest(curl)) {
			logger.log(Level.INFO, "File was already registered: " + curl);
			return;
		}

		try (FileWriter writeFile = new FileWriter(srcPath, true)) {
			writeFile.write("lurl" + ": " + lurl + "\n");
			writeFile.write("LHCPeriod" + ": " + period + "\n");
			writeFile.write("run" + ": " + run + "\n");
			writeFile.write("seName" + ": " + seName + "\n");
			writeFile.write("seioDaemons" + ": " + seioDaemons + "\n");
			writeFile.write("surl" + ": " + surl + "\n");
			writeFile.write("curl" + ": " + curl + "\n");
			writeFile.write("guid" + ": " + guid + "\n");
			writeFile.write("size" + ": " + size + "\n");
			writeFile.write("ctime" + ": " + ctime + "\n");

			SE se = new SE(seName, 1, "", "", seioDaemons);
			GUID guidG = new GUID(guid);
			guidG.size = size;
			PFN pfn = new PFN(seioDaemons + "/" + surl, guidG, se);

			try (Timing t = new Timing(monitor, "cp_execution_time")) {
				(new Xrootd()).get(pfn, new File(tmpFile));
			}

			try (Timing t = new Timing(monitor, "md5_execution_time")) {
				md5Output = IOUtils.getMD5(new File(tmpFile));
			}

			logger.log(Level.INFO, "MD5 checksum for the file " + surl + " is " + md5Output);
			writeFile.write("md5" + ": " + md5Output + "\n");

			if (!new File(tmpFile).delete())
				logger.log(Level.WARNING, "Could not delete file " + tmpFile);

			Main.nrFilesCreated.getAndIncrement();
			logger.log(Level.INFO, "Total number of metadata files successfully created: "
					+ Main.nrFilesCreated.get());
			monitor.incrementCounter("nr_metafiles_created");
			monitor.addMeasurement("nr_processed_bytes", size);
		}
		catch (IOException e) {
			logger.log(Level.WARNING, "Caught exception while writing the metadata file for " + metaFileName, e);
		}

		Main.moveFile(logger, srcPath, destPath);
	}
}
