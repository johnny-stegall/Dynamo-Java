package dynamo.engines;

import dynamo.FaultTolerant;
import dynamo.data.IDataFactory;
import dynamo.handlers.FileHandler;
import dynamo.handlers.IDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/****************************************************************************
* Generates a random quantity of data within the specified range between
* the specified range of dates/times at one-hour intervals.
****************************************************************************/
public class HourlyRangeEngine<T> implements IDataEngine<T>
{
  private static final int ONE_DAY = 86400;
  private static final long DEFAULT_LOWER_COUNT = 100000;
  private static final long DEFAULT_UPPER_COUNT = 500000;
  private static final Date DEFAULT_START_DATE = new Date(Instant.now().minusSeconds(ONE_DAY * 30).toEpochMilli());
  private static final Date DEFAULT_END_DATE = new Date();
  private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors() * 2;
  private static final int DEFAULT_SLEEPY_TIME = 0;

  private final Logger _logger = LoggerFactory.getLogger(HourlyRangeEngine.class);
  private final IDataFactory<T> _dataFactory;
  private final FaultTolerant _faultTolerant = new FaultTolerant();

  private IDataHandler _dataHandler;
  private long _lowerQuantity;
  private long _upperQuantity;
  private Date _startDate;
  private Date _endDate;
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
  public HourlyRangeEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler)
  {
    this(dataFactory, dataHandler, DEFAULT_LOWER_COUNT, DEFAULT_UPPER_COUNT, DEFAULT_START_DATE, DEFAULT_END_DATE, DEFAULT_THREADS, DEFAULT_SLEEPY_TIME);
  }

  /****************************************************************************
  * Creates an instance of QuantityEngine.
  *
  * @param dataFactory
  *   The data factory.
  * @param dataHandler
  *   The data handler.
  * @param lowerQuantity
  *   The lowest number of items to generate.
  * @param upperQuantity
  *   The highest number of items to generate.
  ****************************************************************************/
  public HourlyRangeEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long lowerQuantity, final long upperQuantity)
  {
    this(dataFactory, dataHandler, lowerQuantity, upperQuantity, DEFAULT_START_DATE, DEFAULT_END_DATE);
  }

  /****************************************************************************
  * Creates an instance of QuantityEngine.
  *
  * @param dataFactory
  *   The data factory.
  * @param dataHandler
  *   The data handler.
  * @param lowerQuantity
  *   The lowest number of items to generate.
  * @param upperQuantity
  *   The highest number of items to generate.
  * @param startDate
  *   The start date.
  * @param endDate
  *   The end date.
  ****************************************************************************/
  public HourlyRangeEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long lowerQuantity, final long upperQuantity, final Date startDate, final Date endDate)
  {
    this(dataFactory, dataHandler, lowerQuantity, upperQuantity, startDate, endDate, DEFAULT_THREADS, DEFAULT_SLEEPY_TIME);
  }

  /****************************************************************************
  * Creates an instance of QuantityEngine.
  *
  * @param dataFactory
  *   The data factory.
  * @param dataHandler
  *   The data handler.
  * @param lowerQuantity
  *   The lowest number of items to generate.
  * @param upperQuantity
  *   The highest number of items to generate.
  * @param startDate
  *   The start date.
  * @param endDate
  *   The end date.
  * @param threadCount
  *   The number of CPU threads to use to generate data.
  ****************************************************************************/
  public HourlyRangeEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long lowerQuantity, final long upperQuantity, final Date startDate, final Date endDate, final int threadCount)
  {
    this(dataFactory, dataHandler, lowerQuantity, upperQuantity, startDate, endDate, threadCount, DEFAULT_SLEEPY_TIME);
  }

  /****************************************************************************
  * Creates an instance of QuantityEngine.
  *
  * @param dataFactory
  *   The data factory.
  * @param dataHandler
  *   The data handler.
  * @param lowerQuantity
  *   The lowest number of items to generate.
  * @param upperQuantity
  *   The highest number of items to generate.
  * @param startDate
  *   The start date.
  * @param endDate
  *   The end date.
  * @param threadCount
  *   The number of CPU threads to use to generate data.
  * @param sleepyTime
  *   The number of milliseconds to wait between each iteration.
  ****************************************************************************/
  public HourlyRangeEngine(final IDataFactory<T> dataFactory, final IDataHandler dataHandler, final long lowerQuantity, final long upperQuantity, final Date startDate, final Date endDate, final int threadCount, final int sleepyTime)
  {
    _dataFactory = dataFactory;
    _dataHandler = dataHandler;
    _lowerQuantity = lowerQuantity;
    _upperQuantity = upperQuantity;
    _startDate = startDate;
    _endDate = endDate;
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
      Date currentDate = _startDate;

      while (currentDate.before(_endDate))
      {
        final Date dataTimestamp = currentDate;
        final long quantity = ThreadLocalRandom.current().nextLong(_lowerQuantity, _upperQuantity);
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(dataTimestamp);

        threadPool.execute(() ->
        {
          for (long itemIndex = 0; itemIndex < quantity; itemIndex++)
          {
            final T data = _dataFactory.Create();

            if (_dataHandler instanceof FileHandler)
            {
              _dataHandler = new FileHandler(String.valueOf(calendar.get(Calendar.YEAR))
                + "\\"
                + String.format("%02d", calendar.get(Calendar.MONTH) + 1)
                + "\\"
                + String.format("%02d", calendar.get(Calendar.DATE))
                + "\\"
                + String.format("%02d00", calendar.get(Calendar.HOUR_OF_DAY)));
            }

            try
            {
              _faultTolerant.retryOnFailure(_dataHandler, data);
            }
            catch (Throwable ie)
            {
              _logger.error(ie.getMessage(), ie);
            }
          }
        });

        Thread.sleep(_sleepyTime);
        currentDate.setTime(currentDate.toInstant().plusSeconds(3600).toEpochMilli());
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

      if (_lowerQuantity == DEFAULT_LOWER_COUNT)
      {
        final String quantity = settings.getProperty("Engines.HourlyRange.LowerQuantity");

        if (quantity != null && !quantity.isEmpty())
          _lowerQuantity = Long.parseLong(quantity);
      }

      if (_upperQuantity == DEFAULT_UPPER_COUNT)
      {
        final String quantity = settings.getProperty("Engines.HourlyRange.UpperQuantity");

        if (quantity != null && !quantity.isEmpty())
          _upperQuantity = Long.parseLong(quantity);
      }

      if (_startDate == DEFAULT_START_DATE)
      {
        final String startDate = settings.getProperty("Engines.HourlyRange.StartDate");

        if (startDate != null && !startDate.isEmpty())
        {
          final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
          final LocalDate parsedDate = LocalDate.parse(startDate, dateFormat);
          _startDate = Date.from(parsedDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
      }

      if (_endDate == DEFAULT_END_DATE)
      {
        final String endDate = settings.getProperty("Engines.HourlyRange.EndDate");

        if (endDate != null && !endDate.isEmpty())
        {
          final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd");
          final LocalDate parsedDate = LocalDate.parse(endDate, dateFormat);
          _endDate = Date.from(parsedDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
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