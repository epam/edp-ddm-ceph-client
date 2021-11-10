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

- Import `com.epam.digital.data.platform.integration.ceph.config.CephConfig` to your config
- Define these properties in your project 
```properties
ceph.http-endpoint=
ceph.access-key=
ceph.secret-key=
```
- Inject `com.epam.digital.data.platform.integration.ceph.service.CephService` to your service
- Make sure the bucket you're using exists, or you will get `MisconfigurationException`
- Be aware of all amazon exceptions wrapped by `CephCommunicationException`

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

The ddm-ceph-client is released under version 2.0 of
the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).