package dynamo.handlers;

/******************************************************************************
* Interface to be implemented by any dynamo data handler.
******************************************************************************/
public interface IDataHandler
{
  /****************************************************************************
  * Handles generated data.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable;
}
