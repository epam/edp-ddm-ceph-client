# ddm-ceph-client
This is a library used for reading and writing content to ceph

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

###Postman
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