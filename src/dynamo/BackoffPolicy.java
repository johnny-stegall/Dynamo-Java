package dynamo;

/******************************************************************************
* The backoff policy for methods that can be retried.
******************************************************************************/
public enum BackoffPolicy
{
  STATIC,
  LINEAR,
  EXPONENTIAL,
  RANDOM
}
