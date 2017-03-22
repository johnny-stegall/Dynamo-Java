package dynamo.engines;

import dynamo.FaultTolerant;
import dynamo.data.IDataFactory;
import dynamo.handlers.IDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/****************************************************************************
* Generates the specified quantity of data using a multi-threaded loop.
****************************************************************************/
public class QuantityEngine<T> implements IDataEngine<T>
{
  private static final long DEFAULT_COUNT = 100000;
  private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() * 2;
  private static final int DEFAULT_SLEEPY_TIME = 0;

  private final Logger _logger = LoggerFactory.getLogger(QuantityEngine.class);
  private final IDataHandler _dataHandler;
  private final IDataFactory<T> _dataFactory;
  private final FaultTolerant _faultTolerant = new FaultTolerant();

  private long _quantity;
  private int _threadCount;
  private int _sleepyTime;

  /****************************************************************************
  * Creates an instance of QuantityEngine.
  *
  * @param dataFactory
  *   The data factory.
  * @param dataHandler
  *   The data handler.
  ****************************************************************************/
  public QuantityEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler)
  {
    this(dataFactory, dataHandler, DEFAULT_COUNT);
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
  public QuantityEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long quantity)
  {
    this(dataFactory, dataHandler, quantity, DEFAULT_THREADS);
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
  public QuantityEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long quantity, int threadCount)
  {
    this(dataFactory, dataHandler, quantity, threadCount, DEFAULT_SLEEPY_TIME);
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
  public QuantityEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, long quantity, int threadCount, int sleepyTime)
  {
    _dataFactory = dataFactory;
    _dataHandler = dataHandler;
    _quantity = quantity;
    _threadCount = threadCount;
    _sleepyTime = sleepyTime;

    CheckConfiguration();
  }

  /****************************************************************************
  * Generates the specified quantity of T using the specified number of
  * threads and wait time for throttling.
  ****************************************************************************/
  public void ProduceData()
  {
    final ExecutorService threadPool = Executors.newFixedThreadPool(_threadCount);

    try
    {
      for (long itemIndex = 0; itemIndex < _quantity; itemIndex++)
      {
        threadPool.execute(() ->
        {
          try
          {
            final T data = _dataFactory.Create();
            _faultTolerant.retryOnFailure(_dataHandler, data);
          }
          catch (Throwable ie)
          {
            _logger.error(ie.getMessage(), ie);
          }
        });

        Thread.sleep(_sleepyTime);
      }

      threadPool.shutdown();
      threadPool.awaitTermination(12, TimeUnit.HOURS);
    }
    catch (Exception e)
    {
      _logger.error(e.getMessage(), e);
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

      if (_quantity == DEFAULT_COUNT)
      {
        final String quantity = settings.getProperty("Engines.Quantity.Quantity");

        if (quantity != null && !quantity.isEmpty())
          _quantity = Long.parseLong(quantity);
      }

      if (_threadCount == DEFAULT_THREADS)
      {
        final String threads = settings.getProperty("Engines.Quantity.Threads");

        if (threads != null && !threads.isEmpty())
          _threadCount = Integer.parseInt(threads);
      }

      if (_sleepyTime == DEFAULT_SLEEPY_TIME)
      {
        final String sleepyTime = settings.getProperty("Engines.Quantity.SleepyTime");

        if (sleepyTime != null && !sleepyTime.isEmpty())
          _sleepyTime = Integer.parseInt(sleepyTime);
      }
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
}