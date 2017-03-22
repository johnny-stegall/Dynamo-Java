package dynamo;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class AmbariRestClient
{
  private static final int ZOOKEEPER_PORT = 2181;
  private static final int KAFKA_BROKER_PORT = 9092;

  private final String _ambariUrl;
  private final String _cluster;
  private final Gson _gson = new Gson();

  private HttpClientContext _httpClientContext;

  /****************************************************************************
  * Creates an instance of AmbariRestClient.
  *
  * @param ambariUrl
  *   The Ambari URL.
  * @param clusterName
  *   The cluster name.
  * @param username
  *   The cluster username.
  * @param password
  *   The cluster password.
  ****************************************************************************/
  public AmbariRestClient(String ambariUrl, String clusterName, String username, String password)
  {
    _ambariUrl = ambariUrl;
    _cluster = clusterName;

    final CredentialsProvider basicProvider = new BasicCredentialsProvider();
    final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);

    basicProvider.setCredentials(new AuthScope(getAmbariHostName(), 443), credentials);

    _httpClientContext = HttpClientContext.create();
    _httpClientContext.setCredentialsProvider(basicProvider);
  }

  /****************************************************************************
  * Constructs the Ambari endpoint URL.
  ****************************************************************************/
  public String getAmbariEndpointUrl()
  {
    return String.format("https://%1$s/%2$s",
      getAmbariHostName(),
      String.format("api/v1/clusters/%1$s/services/", _cluster));
  }

  /****************************************************************************
  * Gets the Kafka Broker nodes.
  ****************************************************************************/
  public ArrayList<String> getKafkaNodes()
  {
    return queryHosts(getKafkaEndpointUrl(), KAFKA_BROKER_PORT);
  }

  /****************************************************************************
  * Gets the Zookeeper nodes.
  ****************************************************************************/
  public ArrayList<String> getZookeeperNodes()
  {
    return queryHosts(getZookeeperEndpointUrl(), ZOOKEEPER_PORT);
  }

  /****************************************************************************
  * Gets the Ambari host name.
  ****************************************************************************/
  private String getAmbariHostName()
  {
    return String.format("%1$s.%2$s", _cluster, _ambariUrl);
  }

  /****************************************************************************
  * Constructs the Kafka endpoint URL.
  ****************************************************************************/
  private String getKafkaEndpointUrl()
  {
    return getAmbariEndpointUrl() + "KAFKA/components/KAFKA_BROKER";
  }

  /****************************************************************************
  * Constructs the Zookeeper endpoint URL.
  ****************************************************************************/
  private String getZookeeperEndpointUrl()
  {
    return getAmbariEndpointUrl() + "ZOOKEEPER/components/ZOOKEEPER_SERVER";
  }

  /****************************************************************************
  * Queries the cluster for the specified information.
  *
  * @param componentUrl
  *   The component URL.
  * @param port
  *   The port.
  ****************************************************************************/
  private ArrayList<String> queryHosts(String componentUrl, int port)
  {
    final ArrayList<String> nodeNames = new ArrayList<>();
    final CloseableHttpClient httpClient = HttpClients.createDefault();
    final HttpGet httpget = new HttpGet(componentUrl);
    CloseableHttpResponse response = null;

    try
    {
      response = httpClient.execute(httpget, _httpClientContext);

      final HttpEntity httpEntity = response.getEntity();
      final InputStreamReader streamReader = new InputStreamReader(httpEntity.getContent());
      final String jsonContent = IOUtils.toString(streamReader);

      final JsonObject data = _gson.fromJson(jsonContent, JsonObject.class);
      final JsonArray hosts = data.getAsJsonArray("host_components");

      for (int hostIndex = 0; hostIndex < hosts.size(); hostIndex++)
      {
        final JsonObject hostInfo = hosts.get(hostIndex).getAsJsonObject();
        final String hostName = hostInfo.getAsJsonObject("HostRoles").get("host_name").getAsString();
        nodeNames.add(String.format("%1s:%2s", hostName, port));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if (response != null)
          response.close();
      }
      catch (IOException e)
      {
        e.printStackTrace();
      }
    }

    return nodeNames;
  }
}