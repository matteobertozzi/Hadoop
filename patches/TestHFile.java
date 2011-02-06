import java.util.Random;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.SimpleBlockCache;

public class TestHFile {
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("mm:ss.SS");
    private long totalBytesWritten;
    private byte[][] kvlist;
    private int nentries;

    public TestHFile (int nentries) {
      populateKVList(nentries);
    }

    public void writePlainNoCache (FileSystem fs, Path path) {
      testWrite(fs, path, Compression.Algorithm.NONE, null);
      testRead(fs, path);
    }

    public void writePlainWithCache (FileSystem fs, Path path) {
      testWrite(fs, path, Compression.Algorithm.NONE, new SimpleBlockCache());
      testRead(fs, path);
    }

    public void writeCompressNoCache (FileSystem fs, Path path) {
      testWrite(fs, path, Compression.getCompressionAlgorithmByName("gz"), null);
      testRead(fs, path);
    }

    public void writeCompressWithCache (FileSystem fs, Path path) {
      testWrite(fs, path, Compression.getCompressionAlgorithmByName("gz"), new SimpleBlockCache());
      testRead(fs, path);
    }

    private void testWrite (FileSystem fs,
                           Path path,
                           Compression.Algorithm compress,
                           BlockCache blockCache)
    {
      long dataInterval = 0;
      long fileInterval = 0;

      try {
        fileInterval = System.currentTimeMillis();

        HFile.Writer writer = new HFile.Writer(fs, path, HFile.DEFAULT_BLOCKSIZE,
                                               compress, null, blockCache);

        // Append Data
        dataInterval = System.currentTimeMillis();
        for (int i = 0; i < kvlist.length; ++i)
          writer.append(kvlist[i], kvlist[i]);
        dataInterval = System.currentTimeMillis() - dataInterval;

        writer.close();
        fileInterval = System.currentTimeMillis() - fileInterval;

        System.out.printf("HSTAT - " + path + " - Data written in %s, Size %.3fMiB/s\n",
                          dateFormatter.format(dataInterval),
                          (float)totalBytesWritten / (1024.0f * 1024.0f));

        System.out.printf("HSTAT - " + path + " - File written in %s, Size %.3fMiB\n",
                          dateFormatter.format(fileInterval),
                          (float)fs.getFileStatus(path).getLen() / (1024.0f * 1024.0f));
      } catch (Exception e) {
        System.out.println("Writer.Something fail..." + e);
      }
    }

    private void testRead (FileSystem fs, Path path) {
      try {
        HFile.Reader reader = new HFile.Reader(fs, path, new SimpleBlockCache(), false, true);
        reader.loadFileInfo();

        HFileScanner scanner = reader.getScanner(false, true);
        scanner.seekTo();

        int i = 0;
        do {
          if (scanner.getKey().compareTo(ByteBuffer.wrap(kvlist[i++])) != 0) {
            System.out.println("FAIL COMPARE IN MEM ON DISK");
            break;
          }
        } while (scanner.next());

        reader.close();
      } catch (Exception e) {
        System.out.println("Reader.Something fail..." + e);
      }
    }

    private void populateKVList(int nentries) {
      totalBytesWritten = 0;
      kvlist = new byte[nentries][];
      for (int i = 0; i < nentries; ++i) {
        kvlist[i] = Bytes.toBytes(String.format("%9d", i));
        totalBytesWritten += kvlist[i].length * 2;

        if (i % 1000000 == 0)
          System.out.println(" - Populate KVList " + i + " - " + getUsedMemory());
      }

      System.out.println("Populated KV List MEMORY " + getUsedMemory());
    }

    private float getUsedMemory() {
      long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
      return(used / 1024.0f / 1024.0f);
    }

    private float calcRate (long size, long time) {
      return(((float)size / (time * 1000.0f)) / (1024.0f));
    }

    public static void main (String[] args) {
      try {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        TestHFile test = new TestHFile(8999999);
        test.writePlainNoCache(fs, new Path("/test-hfile-no-cache"));
        test.writePlainWithCache(fs, new Path("/test-hfile-with-cache"));
        test.writeCompressNoCache(fs, new Path("/test-hfile-gz-no-cache"));
        test.writeCompressWithCache(fs, new Path("/test-hfile-gz-with-cache"));
      } catch (Exception e) {
        System.out.println(e);
      }
    }
}

