[![Codacy Badge](https://app.codacy.com/project/badge/Grade/49b0d1627eec40999b8c163f6df5f157)](https://www.codacy.com/gh/barakb/multi-kafka/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=barakb/multi-kafka&amp;utm_campaign=Badge_Grade)
[![build with gradle](https://github.com/barakb/multi-kafka/actions/workflows/build.yml/badge.svg)](https://github.com/barakb/multi-kafka/actions/workflows/build.yml)
### An example of reactive (non-blocking) program that subscribe to multiple kafka topics

#### The flow

*   subscribe to multiple topic. 
*   buffer by size or time for each stream.
*   combine the buffers to one stream
*   use n threads to process the combined stream
*   unsubscribe from topic (cleanly)


see interface at http://localhost:8080/swagger-ui.html
