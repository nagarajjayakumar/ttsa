package org.hwx.demo.analytics.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Nagaraj Jayakumar
 * IO Utility class 
 *
 */
public class IOUtils {
  private static final int EXTRACT_BUFFER_SIZE = 2048;
  private static final int GZIP_FILE_BUFFER_SIZE = 65536;
  private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

  public static void delete(File file) throws IOException {
    IOUtils.delete(file, false);
  }

  public static void delete(File file, boolean logging) throws IOException {
    if (logging) {
      LOG.info("Delete: " + file.getAbsolutePath());
    }
    if (file.isDirectory()) {
      for (File c : file.listFiles())
        delete(c, logging);
    }
    if (!file.delete()) {
      LOG.error("Failed to delete file: " + file);
    }
  }

  public static boolean exists(String file) {
    // 1) check if file is in jar
    if (IOUtils.class.getClassLoader().getResourceAsStream(file) != null) {
      return true;
    }
    // windows File.separator is \, but getting resources only works with /
    if (IOUtils.class.getClassLoader().getResourceAsStream(
        file.replaceAll("\\\\", "/")) != null) {
      return true;
    }

    // 2) if not found in jar, check the file system
    return new File(file).exists();
  }

  public static InputStream getInputStream(String fileOrUrl) {
    return getInputStream(fileOrUrl, false);
  }

  public static InputStream getInputStream(String fileOrUrl, boolean unzip) {
    InputStream in = null;
    try {
      if (fileOrUrl.matches("https?://.*")) {
        URL u = new URL(fileOrUrl);
        URLConnection uc = u.openConnection();
        in = uc.getInputStream();
      } else {
        // 1) check if file is within jar
        in = IOUtils.class.getClassLoader().getResourceAsStream(fileOrUrl);

        // windows File.separator is \, but getting resources only works with /
        if (in == null) {
          in = IOUtils.class.getClassLoader().getResourceAsStream(
              fileOrUrl.replaceAll("\\\\", "/"));
        }

        // 2) if not found in jar, load from the file system
        if (in == null) {
          in = new FileInputStream(fileOrUrl);
        }
      }

      // unzip if necessary
      if ((unzip) && (fileOrUrl.endsWith(".gz"))) {
        in = new GZIPInputStream(in, GZIP_FILE_BUFFER_SIZE);
      }

      // buffer input stream
      in = new BufferedInputStream(in);

    } catch (FileNotFoundException e) {
      LOG.error("FileNotFoundException: " + e.getMessage());
    } catch (IOException e) {
      LOG.error("IOException: " + e.getMessage());
    }

    return in;
  }

  public static InputStream getInputStream(File file) {
    InputStream in = null;
    try {
      in = new FileInputStream(file);

      // unzip if necessary
      if (file.getName().endsWith(".gz")) {
        in = new GZIPInputStream(in, GZIP_FILE_BUFFER_SIZE);
      }

      // buffer input stream
      in = new BufferedInputStream(in);

    } catch (FileNotFoundException e) {
      LOG.error("FileNotFoundException: " + e.getMessage());
    } catch (IOException e) {
      LOG.error("IOException: " + e.getMessage());
    }

    return in;
  }

  public static void extractTarGz(String inputTarGz, String outDir) {
    extractTarGz(getInputStream(inputTarGz), outDir, false);
  }

  public static void extractTarGz(InputStream inputTarGzStream, String outDir,
      boolean logging) {
    try {
      GzipCompressorInputStream gzIn = new GzipCompressorInputStream(
          inputTarGzStream);
      TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);

      // read Tar entries
      TarArchiveEntry entry = null;
      while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
        if (logging) {
          LOG.info("Extracting: " + outDir + File.separator + entry.getName());
        }
        if (entry.isDirectory()) { // create directory
          File f = new File(outDir + File.separator + entry.getName());
          f.mkdirs();
        } else { // decompress file
          int count;
          byte data[] = new byte[EXTRACT_BUFFER_SIZE];

          FileOutputStream fos = new FileOutputStream(outDir + File.separator
              + entry.getName());
          BufferedOutputStream dest = new BufferedOutputStream(fos,
              EXTRACT_BUFFER_SIZE);
          while ((count = tarIn.read(data, 0, EXTRACT_BUFFER_SIZE)) != -1) {
            dest.write(data, 0, count);
          }
          dest.close();
        }
      }

      // close input stream
      tarIn.close();

    } catch (IOException e) {
      LOG.error("IOException: " + e.getMessage());
    }
  }

}
