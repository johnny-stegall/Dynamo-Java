package dynamo.handlers;

import com.google.gson.Gson;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

/******************************************************************************
* Sends data to MongoDB.
******************************************************************************/
public class MongoDbHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(MongoDbHandler.class);
  private final ISerializer _serializer;
  private final Gson _gson;

  private MongoClient _client;
  private MongoCollection _collection;

  /****************************************************************************
  * Creates an instance of MongoDbHandler.
  ****************************************************************************/
  public MongoDbHandler()
  {
    _gson = new Gson();
    CheckConfiguration();
    _serializer = new JsonSerializer();
  }

  /****************************************************************************
  * Serializes the data to a byte array and sends it into MongoDB as a message.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable
  {
    final Document document = Document.parse(_gson.toJson(data));
    document.putIfAbsent("_id", ObjectId.get());
    _collection.insertOne(document, (Object result, final Throwable me) ->
    {
      if (me != null)
        _logger.error(me.getMessage(), me);
      else
        _logger.info("Document written.");
    });
  }

  /****************************************************************************
  * Destructors aren't reliable in Java, but it's good to call this here
  * anyway.
  ****************************************************************************/
  protected void finalize()
  {
    try
    {
      _client.close();
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

      _client = MongoClients.create(settings.getProperty("Handlers.MongoDB.ConnectionString"));

      CreateDatabaseAndCollection(settings);
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
   * Creates the Document DB database and collection.
   *
   * @param settings
   *   A reference to the properties.
   ****************************************************************************/
  private void CreateDatabaseAndCollection(final Properties settings)
  {
    final String databaseName = settings.getProperty("Handlers.MongoDB.Database");
    final MongoDatabase database = _client.getDatabase(databaseName);

    final String collectionName = settings.getProperty("Handlers.MongoDB.Collection");
    _collection = database.getCollection(collectionName);
  }
}