package analyzer;

import alien.config.ConfigUtils;
import alien.io.xrootd.XrootdFile;
import alien.io.xrootd.XrootdListing;
import alien.io.xrootd.client.XrootdClient;
import spooler.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ListingUtils {
     static String server = Main.listingProperties.gets("seioDaemons", Main.defaultseioDaemons)
            .substring(Main.listingProperties.gets("seioDaemons",
                    Main.defaultseioDaemons).lastIndexOf('/') + 1);
     static String statFileName = "/data/epn2eos_tool/stat_file_size";
     static String statRootDirsFileName = "/data/epn2eos_tool/stat_root_dirs";
    private static Logger logger = ConfigUtils.getLogger(ListingUtils.class.getCanonicalName());

    /*static String getParentName(String path) {
        Path parent = Paths.get(path).getParent();
        return parent.getName(parent.getNameCount() - 1).toString();
    }

    static String getFileName(String dirName, String parent) {
        String localFile = Main.listingProperties.gets("localFile", Main.defaultLocalFile);
        String name = localFile.substring(0, localFile.lastIndexOf('.')) + "-";
        name += (parent  != null) ? (parent + "-" + dirName) : dirName;
        String extension = localFile.substring(localFile.lastIndexOf(".") + 1);
        return String.format("%s.%s", name, extension);
    }*/

    public static Set<String> getScanDirs(String fileName) {
        Set<String> scanDirs = new HashSet<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String path;
            while ((path = reader.readLine()) != null) {
                scanDirs.add(path.trim());
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Caught exception while trying to read from file " + fileName);
        }
        return scanDirs;
    }

    public static Set<XrootdFile> getFirstLevelDirs(String path) throws IOException {
        XrootdListing listing = new XrootdListing(Main.getXrootdClient(), path, null);
        return listing.getDirs();
    }

    public static Pair<Integer, Long> scanFiles(String path) throws MalformedURLException {
        return scanFiles(Main.getXrootdClient(), path, statFileName);
    }
    public static Pair<Integer, Long> scanFiles(XrootdClient server, String path, String outputFileName) {
        XrootdListing listing;
        int nr_files = 0;
        long nr_bytes = 0L;
        try {
            listing = new XrootdListing(server, path);
            Set<XrootdFile> directories = listing.getDirs();
            Set<XrootdFile> listFiles = listing.getFiles();

            try (FileWriter writer = new FileWriter(outputFileName, true)) {
                for (XrootdFile file : listFiles) {
                    nr_files += 1;
                    nr_bytes += file.getSize();

                    writer.write(file.getFullPath() + ", " + file.getSize() + "\n");
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Cannot write to the file: " + ListingUtils.statFileName);
            }

            for (XrootdFile dir : directories) {
                Pair<Integer, Long> stats =  scanFiles(server, dir.getFullPath(), outputFileName);
                nr_files += stats.getFirst();
                nr_bytes += stats.getSecond();
            }
            return new Pair<>(nr_files, nr_bytes);
        } catch (IOException e) {
            //todo
        }

        return new Pair<>(0, 0L);
    }
}
