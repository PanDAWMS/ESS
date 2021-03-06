Event Streaming Service(ESS)

- A streaming service to deliver fine-grained input events access for High Energy Physics.
- A fine-grained content delivery network(CDN) for High Energy Physics.


ESS deployment architecture

- ESS is a fine-grained Content Delivery Network(CDN). It includes one Head Service and multiple Edge Services.
- The Head Service manages bookkeepings, brokerages between different Edge Services.
- The Edge Service heartbeats to the Head Service, preprocessing files(pre-caching, splitting, decompossing, combining and son on)

![Here is the ESS deployment architecture](https://github.com/PanDAWMS/ESS/blob/master/doc/design/images/ESS_fullmap.png)

ESS internal architecture
- Database layer: Central Oracle at CERN to bookkeeps all information.
- ORM(Object-Relational Mapping) layer: Map database items to python objects.
- Core layer:Providing basic access to ORM layer.
- Rest service
  1. Interface for Edge to heartbeat
  2. Interface for Panda to request to pre-cache files
  3. Interface for pilot to get event-level file info of cached files
- Daemons
  1. ResourceManager: manage the cache space
  2. Broker and assigner: broker and assign requests to different Edge services.
  3. PreCacher: Precache files from ddm
  4. Splitter: Split files to fine-grained level
  5. Notifier: Notify Panda that the request is ready

![Here is the ESS internal architecture](https://github.com/PanDAWMS/ESS/blob/master/doc/design/images/ESS_architecture.png)

For more information:  https://github.com/PanDAWMS/ESS/wiki.
