/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.DropDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class TestClient {

  private MilvusServiceClient client;
  private static final OkHttpClient embeddingsClient = new OkHttpClient();
  private static final ObjectMapper mapper = new ObjectMapperProvider().get();

  public TestClient() {
    client =
        new MilvusServiceClient(
            ConnectParam.newBuilder().withHost("localhost").withPort(19530).build());
  }

  public static List<List<Float>> generateEmbeddings(List<String> str) {
    try {
      String body = mapper.writeValueAsString(str);
      RequestBody rBody = RequestBody.create(body.getBytes(), MediaType.parse("application/json"));
      Request request =
          new Request.Builder().url("http://localhost:3030/embeddings").post(rBody).build();

      Response response = embeddingsClient.newCall(request).execute();
      return mapper.readValue(
          response.body().string(),
          new TypeReference<List<List<Float>>>() {
            @Override
            public Type getType() {
              return super.getType();
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createDataBase(String databaseName) {
    System.out.println(
        client.createDatabase(
            CreateDatabaseParam.newBuilder().withDatabaseName(databaseName).build()));
  }

  private void createCollection(String databaseName, String collectionName) {
    FieldType productIdType =
        FieldType.newBuilder()
            .withName("product_id")
            .withMaxLength(256)
            .withDataType(DataType.VarChar)
            .withPrimaryKey(true)
            .build();
    FieldType productReviewVector =
        FieldType.newBuilder()
            .withName("product_review_vector")
            .withDataType(DataType.FloatVector)
            .withDimension(384)
            .build();

    CreateCollectionParam req =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDatabaseName(databaseName)
            .withShardsNum(2)
            .addFieldType(productIdType)
            .addFieldType(productReviewVector)
            .build();
    System.out.println(client.createCollection(req));
  }

  public void vectorizeAndSave(String databaseName, String collectionName)
      throws FileNotFoundException, IOException, ClassNotFoundException {
    List<String> productIds = new ArrayList<>();
    List<String> reviews = new ArrayList<>();
    File file = new File("/Users/minukolunu/cleaned_products.csv");
    Scanner scanner = new Scanner(file);
    scanner.next();
    while (scanner.hasNext()) {
      String line = scanner.nextLine();
      String[] row = line.split(",");
      String productId = row[0];
      String review = row[row.length - 1];
      if (productId.isEmpty()) {
        continue;
      }
      productIds.add(productId);
      reviews.add(review);
    }
    List<List<Float>> reviewVectors = generateEmbeddings(reviews);

    /*List<List<Float>> reviewVectors = readFromFile("/Users/minukolunu/vectors.txt");*/

    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("product_id", productIds));
    fields.add(new InsertParam.Field("product_review_vector", reviewVectors));

    InsertParam insertParam =
        InsertParam.newBuilder()
            .withCollectionName(collectionName)
            .withDatabaseName(databaseName)
            .withFields(fields)
            .build();
    System.out.println(client.insert(insertParam));
  }

  private void createIndex(String databaseName, String collectionName, String indexField) {
    final IndexType INDEX_TYPE = IndexType.IVF_FLAT; // IndexType
    final String INDEX_PARAM = "{\"nlist\":1024}"; // ExtraParam

    System.out.println(
        client.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withDatabaseName(databaseName)
                .withFieldName(indexField)
                .withIndexType(INDEX_TYPE)
                .withMetricType(MetricType.L2)
                .withExtraParam(INDEX_PARAM)
                .withSyncMode(Boolean.FALSE)
                .build()));
  }

  private List<List<Float>> readFromFile(String fileName)
      throws IOException, ClassNotFoundException {
    FileInputStream fileInputStream = new FileInputStream(fileName);
    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
    List<List<Double>> list = (List<List<Double>>) objectInputStream.readObject();
    objectInputStream.close();
    List<List<Float>> ret = new ArrayList<>();
    for (List<Double> l : list) {
      List<Float> r1 = new ArrayList<>();
      for (Double d : l) {
        r1.add(d.floatValue());
      }
      ret.add(r1);
    }
    return ret;
  }

  private void saveToFile(List<List<Double>> reviewVectors) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream("/Users/minukolunu/vectors.txt");
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
    objectOutputStream.writeObject(reviewVectors);
    objectOutputStream.flush();
    objectOutputStream.close();
  }

  public static void main(String[] args)
      throws FileNotFoundException, IOException, ClassNotFoundException {
    String databaseName = "products", collectionName = "product_reviews";
    TestClient client = new TestClient();
    client.dropAll(databaseName, collectionName);
    client.createDataBase(databaseName);
    client.createCollection(databaseName, collectionName);
    client.vectorizeAndSave(databaseName, collectionName);
    client.createIndex(databaseName, collectionName, "product_review_vector");
    System.out.println("Done");
    /*System.out.println(generateEmbeddings(Arrays.asList("asdf", "asdf1")));*/
  }

  private void dropAll(String databaseName, String collectionName) {
    System.out.println(
        client.dropCollection(
            DropCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDatabaseName(databaseName)
                .build()));
    System.out.println(
        client.dropDatabase(DropDatabaseParam.newBuilder().withDatabaseName(databaseName).build()));
  }
}
