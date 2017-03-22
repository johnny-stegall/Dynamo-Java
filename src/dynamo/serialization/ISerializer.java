package dynamo.serialization;

/******************************************************************************
* Interface to be implemented by any dynamo serializer.
******************************************************************************/
public interface ISerializer<T>
{
  /****************************************************************************
  * Serializes an object.
  *
  * @param data
  *   The serialized data.
  * @param typeClass
  *   The generic type's class.
  * @return
  *   The object.
  * @throws Throwable
  *   Any exception that occurs while deserializing.
  ****************************************************************************/
  public T Deserialize(final String data, final Class<T> typeClass) throws Exception;

  /****************************************************************************
  * Serializes an object.
  *
  * @param data
  *   The object.
  * @return
  *   The serialized data in a byte array.
  * @throws Throwable
  *   Any exception that occurs while serializing.
  ****************************************************************************/
  public byte[] Serialize(final T data) throws Exception;

  /****************************************************************************
  * Serializes an object to file. If the file exists, it will be appended to.
  *
  * @param data
  *   The object.
  * @param filename
  *   The full path to the output file.
  * @throws Throwable
  *   Any exception that occurs while serializing.
  ****************************************************************************/
  public void Serialize(final T data, final String filename) throws Exception;
}
