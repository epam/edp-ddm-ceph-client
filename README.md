# ddm-ceph-client

### Overview

* This is a library used for reading and writing content to ceph

### Usage

- Specify dependency in your service:

```xml

<dependencies>
  ...
  <dependency>
    <groupId>com.epam.digital.data.platform</groupId>
    <artifactId>ddm-ceph-client</artifactId>
    <version>...</version>
  </dependency>
  ...
</dependencies>
```

- Choose and create `com.epam.digital.data.platform.integration.ceph.service.CephService` implementation(list of available implementations below).
- Inject `com.epam.digital.data.platform.integration.ceph.legacy.service.CephService` to your service
- Make sure the bucket you're using exists, or you will get `MisconfigurationException`
- Be aware of all amazon exceptions wrapped by `CephCommunicationException`

### Available CephService Implementations:
- `com.epam.digital.data.platform.integration.ceph.service.impl.CephServiceS3Impl` (Amazon S3)  
The service uses Amazon S3 as a storage. There are two options available for creation this service:
  - Declaration the bean using builder:
    ```java
    @Bean
    public CephService cephService(
      @Value("${<property-path>}") String cephHttpEndpoint,
      @Value("${<property-path>}") String cephAccessKey,
      @Value("${<property-path>}") String cephSecretKey) {
    return CephServiceS3Impl.builder()
          .cephEndpoint(cephHttpEndpoint)
          .cephAccessKey(cephAccessKey)
          .cephSecretKey(cephSecretKey)
          .build();
    }
    ```
    In this case `@PostConstruct` runs and builds AmazonS3 client object using specified properties.
  - Create service using constructor and set AmazonS3 client directly:
    ```java
      var amazonS3Client = AmazonS3ClientBuilder.standard(). 
        ... 
        .build()
    
      new CephServiceS3Impl(amazonS3Client);
    ```
### Test execution

* Tests could be run via maven command:
  * `mvn verify` OR using appropriate functions of your IDE.
  
### Postman

 To check GET/PUT endpoints:
- Import `ceph.postman_collection.json` to your postman
- Set next environments:
```
{{base-url}}
{{bucket-name}}
{{document-key}}
{{access-key}}
{{secret-key}}
```

### License

The ddm-ceph-client is Open Source software released under
the [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0).