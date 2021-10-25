package net.quasardb.qdb.jni;

import java.lang.management.ManagementFactory;
import java.io.*;
import java.nio.file.*;
import java.net.*;

class NativeLibraryLoader {
  public static void load(String name) {
      final boolean useCritical = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-Xcomp") > 0;

      Path localFile = getResourceAsLocalFile(name);
      loadLibrary(localFile);
  }

  public static Path getResourceAsLocalFile(String name) {
    URL url = NativeLibraryLoader.class.getResource(name);
    if (url == null)
      throw new RuntimeException("Cannot find native library in classpath: " + name);

    if (url.getProtocol().equals("file"))
      return convertURLtoPath(url);

    if (url.getProtocol().equals("jar"))
      return JarFileHelper.extract(url);

    throw new RuntimeException("Don't now how to extract: " + url);
  }

  private static Path convertURLtoPath(URL url) {
    try {
      return Paths.get(url.toURI());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get path of " + url + ": " + e, e);
    }
  }

  private static void loadLibrary(Path path) {
    try {
      System.load(path.toString());
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to load " + path + ": " + e.getMessage(), e);
    }
  }
}
