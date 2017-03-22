package dynamo;

import dynamo.data.IDataFactory;
import dynamo.engines.IDataEngine;
import dynamo.handlers.IDataHandler;

public class Dynamo
{
  private static IDataEngine _dataEngine;
  private static IDataHandler _dataHandler;

  /**************************************************************************
  * Main entry into the application. Accepts the following arguments:
  *
  * 1. Data Engine - The engine to use to generate data.
  * 2. Data Type - The name of the data type (including namespace).
  * 3. Data Handler - The data handler.
  **************************************************************************/
  public static void main(String[] args)
  {
    try
    {
      if (args.length != 3)
        throw new IllegalArgumentException("Usage: dynamo {data engine} {data type} {data handler}");

      _dataHandler = GetDataHandler(args[2]);
      _dataEngine = GetDataEngine(args[0], args[1]);
      _dataEngine.ProduceData();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }

  /**************************************************************************
  * Gets a data engine using its fully qualified class name.
  *
  * @param engineName
  *   The fully qualified IDataEngine class name.
  * @param engineName
  *   The fully qualified class name of the data type.
  * @return
  *   A class that implements IDataEngine.
  **************************************************************************/
  private static IDataEngine GetDataEngine(String engineName, String dataType) throws Exception
  {
    String factoryClass = dataType.substring(dataType.lastIndexOf(".") + 1) + "Factory";
    IDataFactory dataFactory;

    if (factoryClass.endsWith("ObjectFactoryFactory"))
      dataFactory = (IDataFactory)Class.forName(dataType).newInstance();
    else
      dataFactory = (IDataFactory)Class.forName(dataType + "$" + factoryClass).newInstance();

    final IDataEngine dataEngine = (IDataEngine)Class
      .forName(engineName)
      .getDeclaredConstructor(IDataFactory.class, IDataHandler.class)
      .newInstance(dataFactory, _dataHandler);

    return dataEngine;
  }

  /**************************************************************************
  * Gets a data handler using its fully qualified class name.
  *
  * @param className
  *   The fully qualified IDataHandler class name.
  * @return
  *   A class that implements IDataHandler.
  **************************************************************************/
  private static IDataHandler GetDataHandler(String className) throws Exception
  {
    return (IDataHandler)Class.forName(className).newInstance();
  }
}
