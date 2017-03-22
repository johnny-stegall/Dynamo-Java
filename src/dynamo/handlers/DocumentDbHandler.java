package dynamo.handlers;

import com.microsoft.azure.documentdb.*;
import dynamo.serialization.ISerializer;
import dynamo.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/******************************************************************************
* Sends data to Azure DocumentDB.
******************************************************************************/
public class DocumentDbHandler implements IDataHandler
{
  private final Logger _logger = LoggerFactory.getLogger(DocumentDbHandler.class);
  private final ISerializer _serializer;

  private DocumentClient _client;
  private String _collectionLink;
  private boolean _disableIdGeneration;

  /****************************************************************************
  * Creates an instance of DocumentDBHandler.
  ****************************************************************************/
  public DocumentDbHandler()
  {
    CheckConfiguration();
    _serializer = new JsonSerializer();
  }

  /****************************************************************************
  * Serializes the data to a byte array and sends it into DocumentDB as a
  * message.
  *
  * @param data
  *   The data.
  * @throws Throwable
  *   Any uncaught exception.
  ****************************************************************************/
  public void HandleData(Object data) throws Throwable
  {
    _client.createDocument(_collectionLink, data, null, _disableIdGeneration);
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

      final String connectionString = settings.getProperty("Handlers.DocumentDB.ConnectionString");
      final String masterKey = settings.getProperty("Handlers.DocumentDB.MasterKey");
      _disableIdGeneration = Boolean.parseBoolean(settings.getProperty("Handlers.DocumentDB.DisableIdGeneration", "true"));

      _client = new DocumentClient(connectionString,
        masterKey,
        new ConnectionPolicy(),
        ConsistencyLevel.Session);

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
          e.printStackTrace();
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
  private void CreateDatabaseAndCollection(final Properties settings) throws DocumentClientException
  {
    final String databaseName = settings.getProperty("Handlers.DocumentDB.Database");
    final List<Database> databases = _client
      .queryDatabases("SELECT * FROM root R where R.id = '" + databaseName + "'", null)
      .getQueryIterable()
      .toList();

    if (databases.size() < 1)
    {
      final Database database = new Database();
      database.setId(databaseName);
      _client.createDatabase(database, null);
    }

    final String collectionName = settings.getProperty("Handlers.DocumentDB.Collection");
    final List<DocumentCollection> collections = _client
      .queryCollections("/dbs/" + databaseName, "SELECT * FROM root R where R.id = '" + collectionName + "'", null)
      .getQueryIterable()
      .toList();

    if (collections.size() < 1)
    {
      // Collections can be reserved with throughput specified in request units/second
      final RequestOptions requestOptions = new RequestOptions();
      final int requestUnits = Integer.parseInt(settings.getProperty("Handlers.DocumentDB.RequestUnits", "5000"));
      requestOptions.setOfferThroughput(requestUnits);

      final DocumentCollection collection = new DocumentCollection();
      collection.setId(collectionName);
      _client.createCollection("/dbs/" + databaseName, collection, requestOptions);
    }

    _collectionLink = "/dbs/" + databaseName + "/colls/" + collectionName;
  }
}