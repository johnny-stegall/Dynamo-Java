package dynamo.engines;

import dynamo.FaultTolerant;
import dynamo.data.IDataFactory;
import dynamo.handlers.IDataHandler;
import dynamo.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/****************************************************************************
* Generates data by reading in files using the specified path and filename
* or wildcard pattern.
****************************************************************************/
public class ReplayEngine<T> implements IDataEngine<T>
{
  private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() / 2;

  private final Logger _logger = LoggerFactory.getLogger(ReplayEngine.class);
  private final IDataHandler _dataHandler;
  private final IDataFactory<T> _dataFactory;
  private final FaultTolerant _faultTolerant = new FaultTolerant();

  private int _threadCount;
  private File _directory;
  private File[] _files;

  /****************************************************************************
   * Creates an instance of QuantityEngine.
   *
   * @param dataFactory
   *   The data factory.
   * @param dataHandler
   *   The data handler.
   ****************************************************************************/
  public ReplayEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler)
  {
    this(dataFactory, dataHandler, null, null, DEFAULT_THREADS);
  }

  /****************************************************************************
   * Creates an instance of QuantityEngine.
   *
   * @param dataFactory
   *   The data factory.
   * @param dataHandler
   *   The data handler.
   * @param quantity
   *   The number of items to generate.
   ****************************************************************************/
  public ReplayEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final String path)
  {
    this(dataFactory, dataHandler, path, null, DEFAULT_THREADS);
  }

  /****************************************************************************
   * Creates an instance of QuantityEngine.
   *
   * @param dataFactory
   *   The data factory.
   * @param dataHandler
   *   The data handler.
   * @param quantity
   *   The number of items to generate.
   * @param threadCount
   *   The number of CPU threads to use to generate data.
   ****************************************************************************/
  public ReplayEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final String path, final String files)
  {
    this(dataFactory, dataHandler, path, files, DEFAULT_THREADS);
  }

  /****************************************************************************
   * Creates an instance of QuantityEngine.
   *
   * @param dataFactory
   *   The data factory.
   * @param dataHandler
   *   The data handler.
   * @param quantity
   *   The number of items to generate.
   * @param threadCount
   *   The number of CPU threads to use to generate data.
   * @param sleepyTime
   *   The number of milliseconds to wait between each iteration.
   ****************************************************************************/
  public ReplayEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final String path, final String files, final int threadCount)
  {
    _dataFactory = dataFactory;
    _dataHandler = dataHandler;
    _threadCount = threadCount;

    if (path != null && !path.isEmpty())
      _directory = new File(path);

    if (files != null && !files.isEmpty())
      _files = _directory.listFiles((d, f) -> f.contains(files));

    CheckConfiguration();
  }

  /****************************************************************************
  * Generates the specified quantity of T using the specified number of
  * threads and wait time for throttling.
  ****************************************************************************/
  public void ProduceData()
  {
    final ExecutorService threadPool = Executors.newFixedThreadPool(_threadCount);
    final Class<?> typeClass = _dataFactory.Create().getClass();

    try
    {
      for (final File file : _files)
      {
        try
        {
          threadPool.submit(() ->
          {
            _logger.info("Replaying from file: " + file.getName());
            final ISerializer serializer = ConfigureSerializer(file.toPath());

            try (final BufferedReader bufferedReader = Files.newBufferedReader(file.toPath()))
            {
              String line;
              while((line = bufferedReader.readLine()) != null && !line.isEmpty())
              {
                try
                {
                  final T data = (T)serializer.Deserialize(line, typeClass);
                  _faultTolerant.retryOnFailure(_dataHandler, data);
                }
                catch (Throwable e)
                {
                  _logger.error(e.getMessage(), e);
                }
              }
            }
            catch (IOException ie)
            {
              _logger.error(ie.getMessage(), ie);
            }
          });
        }
        catch (IllegalArgumentException iae)
        {
          _logger.error(iae.getMessage(), iae);
        }
      }

      threadPool.shutdown();
      threadPool.awaitTermination(12, TimeUnit.HOURS);
    }
    catch (InterruptedException ie)
    {
      _logger.error(ie.getMessage(), ie);
    }
  }

  /****************************************************************************
  * Checks for a properties file to load settings.
  ****************************************************************************/
  private void CheckConfiguration()
  {
    InputStream propertiesStream = null;

    try
    {
      final String propertiesFile = Paths.get("dynamo.properties").toAbsolutePath().toString();
      propertiesStream = new FileInputStream(propertiesFile);
      final Properties settings = new Properties();
      settings.load(propertiesStream);

      if (_threadCount == DEFAULT_THREADS)
      {
        final String threads = settings.getProperty("Engines.Replay.Threads");

        if (threads != null && !threads.isEmpty())
          _threadCount = Integer.parseInt(threads);
      }

      final String path = settings.getProperty("Engines.Replay.Path");
      final String files = settings.getProperty("Engines.Replay.Files");

      if (path == null || path.isEmpty())
        throw new IllegalArgumentException("Replay path is empty.");
      else if (files == null || files.isEmpty())
        throw new IllegalArgumentException("Replay files are empty.");

      _directory = new File(path);
      _files = _directory.listFiles((d, f) -> f.contains(files));
    }
    catch (Exception e)
    {
      _logger.error(e.getMessage(), e);
      System.exit(0);
    }
    finally
    {
      if (propertiesStream != null)
      {
        try
        {
          propertiesStream.close();
        }
        catch (Exception e)
        {
          _logger.error(e.getMessage(), e);
        }
      }
    }
  }

  /****************************************************************************
  * Sets up serialization from configuration settings.
  *
  * @param filePath
  *   The path to the file.
  * @return
  *   The serializer.
  * @throw IllegalArgumentException
  *   Thrown on unsupported file formats.
  ****************************************************************************/
  private ISerializer ConfigureSerializer(final Path filePath) throws IllegalArgumentException
  {
    if (filePath.toString().endsWith("avro"))
      return new AvroSerializer();
    else if (filePath.toString().endsWith("csv"))
      return new TextSerializer(",");
    else if (filePath.toString().endsWith("json"))
      return new JsonSerializer();
    else if (filePath.toString().endsWith("tsv"))
      return new TextSerializer("\t");
    else if (filePath.toString().endsWith("xml"))
      return new XmlSerializer();
    else
      throw new IllegalArgumentException("Unsupported file format.");
    }
}
