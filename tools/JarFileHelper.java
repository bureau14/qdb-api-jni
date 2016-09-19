package net.quasardb.qdb.jni;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.jar.*;

class JarFileHelper {
  private static final Path tmpDirectory;

  static { tmpDirectory = Paths.get(System.getProperty("java.io.tmpdir")); }

  public static Path extract(URL resourceUrl) {
    try {
      JarURLConnection jarUrlConnection = (JarURLConnection)resourceUrl.openConnection();

      Path localFile = chooseLocaFile(jarUrlConnection);
      Files.createDirectories(localFile.getParent());

      if (!Files.exists(localFile)) {
        Files.copy(jarUrlConnection.getInputStream(), localFile);
      }

      return localFile;
    } catch (Exception e) {
      throw new RuntimeException("Failed to read " + resourceUrl + ": " + e, e);
    }
  }

  private static Path chooseLocaFile(JarURLConnection jarUrlConnection) throws IOException {
    JarEntry jarEntry = jarUrlConnection.getJarEntry();
    Path folder = Paths.get(jarEntry.getName()).getParent();
    Path fileName = Paths.get(jarEntry.getName()).getFileName();
    String crc = String.format("%08X", jarEntry.getCrc());
    return tmpDirectory.resolve(folder).resolve(crc).resolve(fileName);
  }
}
