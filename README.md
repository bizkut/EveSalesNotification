Telegram bot for sales orders filled notifications using EVE Swagger Interface (ESI). 

What is ESI?
ESI is a scalable RESTful API with SSO authentication and read/write capabilities. In practice, it powers thousands of applications, serving as the backbone of EVE’s extensive third-party ecosystem.

The third-party development community has built a powerful suite of tools that extend EVE’s gameplay, foster innovation, and even shape in-game economies. Market and trade tools like EVE Market Browser and Janice help traders track market trends, optimize logistics, and compare buy/sell orders in real time. Corporation and alliance management platforms such as Alliance Auth automate recruitment, permissions, and access control for player organizations, ensuring smooth functionality for massive alliances. Intel and mapping tools like DOTLAN and RIFT support strategic movements and enemy tracking, while industry tools like Fuzzworks and Adam4EVE assist with blueprints, asset management, and production chains. Even beyond standalone applications, ESI supports integrations like the EVE Online Excel add-in, which provides seamless access to in-game data for planning and analysis.

ESI is massive in scale. Over 2,350 active third-party applications rely on it, with 42% of active EVE players having at least one character authorized in an ESI-based app. The API handles over 350,000 requests per minute, powering countless player-made resources and dashboards. The sheer volume of traffic to ESI-powered websites is immeasurable - if you’ve ever used a community tool for intel, trading, or planning, you’ve likely interacted with ESI. From trade hubs to war councils, from solo industrialists to massive coalitions, ESI is an essential part of EVE Online’s metagame.

This dev blog takes a closer look at ESI today, the challenges it faces, and the steps being taken to ensure a better future for third-party developers and the players who rely on their tools.

History Lesson
APIs have been an integral part of EVE Online for over a decade, allowing players to extend the game beyond the client and into a vast ecosystem of third-party tools. The EVE Swagger Interface (ESI) is the latest evolution, replacing its predecessors, the XML API and CREST. ESI provides both public and authenticated endpoints, with the latter requiring login via EVE Single Sign-On (SSO) to access character or corporation-specific data.

The launch of the XML API was groundbreaking. At the time, no other game offered such a deep level of programmatic access to its data. Players used it to build some of EVE’s earliest third-party tools, from character planners to market aggregators. There were significant limitations, however. It was read-only, slow to update, and the documentation was lacking.

To address these issues, CREST was introduced, offering a RESTful interface and faster access to live simulation data. However, CREST lacked consistency and scalability, making it clear that a more robust solution was necessary.

Enter ESI
Built on Swagger (now OpenAPI), ESI aggregates API specifications from multiple Kubernetes services into a unified API, handling routing, authentication, input/output validation, and more.

ESI’s introduction coincided with Project Sanguine and the first iteration of EVE Portal in late 2016. These projects laid the foundation for a more advanced server architecture within EVE Online and introduced a message bus paradigm that improved real-time data flow between services. As usage evolved, a higher-performance protocol was needed, leading to the integration of gRPC for faster serialization and communication.

This shift paved the way for Quasar, a technology designed to further modernize EVE’s backend with gRPC, event-driven messaging, and microservices. From the early days of XML APIs to the modern era of ESI, Kubernetes, and Quasar, EVE Online remains at the forefront of game-integrated APIs.

Support for third-party development continues to evolve, and as you’ll see next, there’s still work to be done to ensure that ESI remains robust, reliable, and future-proof.

