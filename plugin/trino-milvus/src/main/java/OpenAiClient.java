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
import com.theokanning.openai.embedding.Embedding;
import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.embedding.EmbeddingResult;
import com.theokanning.openai.service.OpenAiService;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.CreateDatabaseParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class OpenAiClient {

  private MilvusServiceClient client;

  public OpenAiClient() {
    client =
        new MilvusServiceClient(
            ConnectParam.newBuilder().withHost("localhost").withPort(19530).build());
  }

  public static List<List<Float>> generateEmbeddings(List<String> str) {
    String openAiKey = "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";
    OpenAiService service = new OpenAiService(openAiKey);
    EmbeddingRequest embeddingRequest =
        EmbeddingRequest.builder().input(str).model("text-embedding-ada-002").build();
    EmbeddingResult result = service.createEmbeddings(embeddingRequest);
    List<List<Double>> doubles =
        result.getData().stream().map(Embedding::getEmbedding).collect(Collectors.toList());
    List<List<Float>> ret = new ArrayList<>();
    for (List<Double> l : doubles) {
      List<Float> r1 = new ArrayList<>();
      for (Double d : l) {
        r1.add(d.floatValue());
      }
      ret.add(r1);
    }
    return ret;
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
            .withDimension(1536)
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
    //    List<List<Double>> reviewVectors = generateEmbeddings(reviews);

    List<List<Float>> reviewVectors = readFromFile("/Users/minukolunu/vectors.txt");

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
    OpenAiClient client = new OpenAiClient();
    /*client.createDataBase(databaseName);
    client.createCollection(databaseName, collectionName);*/
    //    client.vectorizeAndSave(databaseName, collectionName);
    client.createIndex(databaseName, collectionName, "product_review_vector");
  }
}
