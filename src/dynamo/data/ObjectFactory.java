package dynamo.data;

/**************************************************************************
* Cheater way to create objects instead of passing null in for
* IDataFactory parameters.
**************************************************************************/
public class ObjectFactory implements IDataFactory<Object>
{
  /******************************************************************************
  * Creates an instance of GameEvent.
  ******************************************************************************/
  public Object Create()
  {
    return new Object();
  }
}