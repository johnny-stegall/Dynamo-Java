package dynamo;

import dynamo.handlers.IDataHandler;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/******************************************************************************
* Retries a function in the event of failure. Allows for configurable wait
* time between attempts and exceptions that can be ignored.
******************************************************************************/
public class FaultTolerant
{
  private static final Random RNG = new SecureRandom();

  private final int _attempts;
  private final BackoffPolicy _backoffPolicy;
  private final Class<? extends Throwable>[] _failExceptions;
  private final Class<? extends Throwable>[] _retryExceptions;
  private final boolean _showStackTrace;
  private final int _sleep;
  private final TimeUnit _sleepUnit;

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  ****************************************************************************/
  public FaultTolerant()
  {
    this(3, BackoffPolicy.EXPONENTIAL, new Class[] {}, new Class[] { Throwable.class }, true, 3, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  ****************************************************************************/
  public FaultTolerant(int attempts)
  {
    this(attempts, BackoffPolicy.EXPONENTIAL, new Class[] {}, new Class[] { Throwable.class }, true, 1, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  * @param backoffPolicy
  *   The backoff policy between failures.
  ****************************************************************************/
  public FaultTolerant(int attempts, BackoffPolicy backoffPolicy)
  {
    this(attempts, backoffPolicy, new Class[] {}, new Class[] { Throwable.class }, true, 1, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  * @param backoffPolicy
  *   The backoff policy between failures.
  * @param failExceptions
  *   An array of exceptions that if thrown, should cause failure without
  *   retrying.
  ****************************************************************************/
  public FaultTolerant(int attempts, BackoffPolicy backoffPolicy, Class<? extends Throwable>[] failExceptions)
  {
    this(attempts, backoffPolicy, failExceptions, new Class[] { Throwable.class }, true, 1, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  * @param backoffPolicy
  *   The backoff policy between failures.
  * @param failExceptions
  *   An array of exceptions that if thrown, should cause failure without
  *   retrying.
  * @param retryExceptions
  *   An array of exceptions that if thrown should cause the method to be
  *   executed again.
  ****************************************************************************/
  public FaultTolerant(int attempts, BackoffPolicy backoffPolicy, Class<? extends Throwable>[] failExceptions, Class<? extends Throwable>[] retryExceptions)
  {
    this(attempts, backoffPolicy, failExceptions, retryExceptions, true, 1, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  * @param backoffPolicy
  *   The backoff policy between failures.
  * @param failExceptions
  *   An array of exceptions that if thrown, should cause failure without
  *   retrying.
  * @param retryExceptions
  *   An array of exceptions that if thrown should cause the method to be
  *   executed again.
  * @param showStackTrace
  *   True to show the stack trace of exceptions when failing, false to show
  *   only the message.
  ****************************************************************************/
  public FaultTolerant(int attempts, BackoffPolicy backoffPolicy, Class<? extends Throwable>[] failExceptions, Class<? extends Throwable>[] retryExceptions, boolean showStackTrace)
  {
    this(attempts, backoffPolicy, failExceptions, retryExceptions, showStackTrace, 1, TimeUnit.SECONDS);
  }

  /****************************************************************************
  * Creates an instance of FaultTolerate.
  *
  * @param attempts
  *   The number of attempts to try before failing.
  * @param backoffPolicy
  *   The backoff policy between failures.
  * @param failExceptions
  *   An array of exceptions that if thrown, should cause failure without
  *   retrying.
  * @param retryExceptions
  *   An array of exceptions that if thrown should cause the method to be
  *   executed again.
  * @param showStackTrace
  *   True to show the stack trace of exceptions when failing, false to show
  *   only the message.
  * @param sleep
  *   The number of the unit to sleep between attempts.
  * @param sleepUnit
  *   The unit of time to sleep between attempts.
  ****************************************************************************/
  public FaultTolerant(int attempts, BackoffPolicy backoffPolicy, Class<? extends Throwable>[] failExceptions, Class<? extends Throwable>[] retryExceptions, boolean showStackTrace, int sleep, TimeUnit sleepUnit)
  {
    _attempts = attempts;
    _backoffPolicy = backoffPolicy;
    _failExceptions = failExceptions;
    _retryExceptions = retryExceptions;
    _showStackTrace = showStackTrace;
    _sleep = sleep;
    _sleepUnit = sleepUnit;
  }

  /****************************************************************************
  * Parses a message from a chained exception using recursion.
  *
  * @param exception
  *   An exception.
  * @return
  *   The exception message.
  ****************************************************************************/
  private static String getExceptionMessage(final Throwable exception)
  {
    final StringBuilder text = new StringBuilder();
    text.append(exception.getMessage());

    if (exception.getCause() != null)
      text.append("; ").append(getExceptionMessage(exception.getCause()));

    return text.toString();
  }

  /****************************************************************************
  * Checks if a thrown exception matches the list of exceptions to retry.
  *
  * @param thrown
  *   The thrown exception class.
  * @param exceptions
  *   The exceptions to search.
  * @return
  *   True if the exception should cause a retry, false if it should be thrown.
  ****************************************************************************/
  private static boolean matches(final Class<? extends Throwable> thrown, final Class<? extends Throwable>... exceptions)
  {
    for (final Class<? extends Throwable> exception : exceptions)
    {
      if (exception.isAssignableFrom(thrown))
        return true;
    }

    return false;
  }

  /****************************************************************************
  * Catches exceptions and depending on conditions reinvokes the method or
  * throws the exception.
  *
  * @param dataHandler
  *   The class implementing IDataHandler.
  * @param data
  *   The data to pass to the IDataHandler.
  * @throws Throwable
  *   Any exception thrown by the executed method.
  * @throws InterruptedException
  *   If the thread is interrupted while waiting between retries.
  ****************************************************************************/
  public void retryOnFailure(final IDataHandler dataHandler, final Object data) throws Throwable
  {
    int attempts = 0;

    while (true)
    {
      try
      {
        dataHandler.HandleData(data);
        return;
      }
      catch (Throwable e)
      {
        if (matches(e.getClass(), _failExceptions) || !matches(e.getClass(), _retryExceptions))
          throw e;

        attempts++;

        if (_showStackTrace)
        {
          LoggerFactory
            .getLogger(dataHandler.getClass())
            .warn(e.getMessage(), e);
        }
        else
        {
          LoggerFactory
            .getLogger(dataHandler.getClass())
            .warn(getExceptionMessage(e));
        }

        if (attempts >= _attempts)
        {
          LoggerFactory
            .getLogger(dataHandler.getClass())
            .warn("Maximum attempts of " + _attempts + " reached. Bailing.");

          throw e;
        }

        if (_sleep > 0L)
          sleep(attempts, dataHandler);
      }
    }
  }

  /****************************************************************************
  * Pauses a thread before returning.
  *
  * @param failures
  *   The number of attempts failed.
  * @param dataHandler
  *   The class implementing IDataHandler.
  * @throws InterruptedException
  *   Thrown if the thread is interrupted.
  ****************************************************************************/
  private void sleep(final int failures, final IDataHandler dataHandler) throws InterruptedException
  {
    final long sleepyTime;

    if (_backoffPolicy == BackoffPolicy.LINEAR)
      sleepyTime = _sleep * failures;
    else if (_backoffPolicy == BackoffPolicy.STATIC)
      sleepyTime = _sleep;
    else if (_backoffPolicy == BackoffPolicy.RANDOM)
      sleepyTime = _sleep * RNG.nextInt(2 << failures);
    else
      sleepyTime = _sleep * (failures * failures);

    LoggerFactory
      .getLogger(dataHandler.getClass())
      .warn("Retrying in " + sleepyTime + " " + _sleepUnit.name().toLowerCase() + ".");

    _sleepUnit.sleep(sleepyTime);
  }
}
