### An example of reactive probram that subscribe to multiple kafka topics

##### The flow,

- subscribe to multiple topic. 
- buffer by size or time for each stream.
- combine the buffers to one stream
- use n threads to process to the combined stream
- at we can unsubscribe to a topic

see interface at http://localhost:8080/swagger-ui.html
