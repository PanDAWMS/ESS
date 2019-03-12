Event Streaming Service(ESS)

- A streaming service to deliver fine-grained input events access for High Energy Physics.
- A fine-grained content delivery network(CDN) for High Energy Physics.


ESS deployment architecture

- ESS is a fine-grained Content Delivery Network(CDN).
...It includes one Head Service and multiple Edge Services.
- The Head Service manages bookkeepings, brokerages between different Edge Services.
- The Edge Service heartbeats to the Head Service, preprocessing files(pre-caching, splitting, decompossing, combining and son on)

![Here is the ESS deployment architecture](https://github.com/PanDAWMS/ESS/blob/master/doc/design/images/ESS_fullmap.png)

ESS internal architecture
- Database layer
...Central Oracle at CERN to bookkeeps all information.
- ORM(Object-Relational Mapping) layer
...Map database items to python objects.
- Core layer
...Providing basic access to ORM layer.
- Rest service
...Interface for Edge to heartbeat
...Interface for Panda to request to pre-cache files
...Interface for pilot to get event-level file info of cached files
- Daemons
...ResourceManager: manage the cache space
...Broker and assigner: broker and assign requests to different Edge services.
...PreCacher: Precache files from ddm
...Splitter: Split files to fine-grained level
...Notifier: Notify Panda that the request is ready

![Here is the ESS internal architecture](https://github.com/PanDAWMS/ESS/blob/master/doc/design/images/ESS_architecture.png)

For more information:  https://github.com/PanDAWMS/ESS/wiki.
