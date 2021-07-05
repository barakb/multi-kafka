### An example of reactive probram that subscribe to multiple kafka topics

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7112b7264d3649c8af54f764647bf96b)](https://app.codacy.com/gh/barakb/multi-kafka?utm_source=github.com&utm_medium=referral&utm_content=barakb/multi-kafka&utm_campaign=Badge_Grade_Settings)

##### The flow,

- subscribe to multiple topic. 
- buffer by size or time for each stream.
- combine the buffers to one stream
- use n threads to process to the combined stream
- at we can unsubscribe to a topic

see interface at http://localhost:8080/swagger-ui.html
