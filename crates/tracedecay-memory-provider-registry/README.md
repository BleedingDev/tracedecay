# TraceDecay Memory Provider Registry

This product-owned crate is the single concrete composition boundary for the default-off Memory Fabric feature.

It accepts the existing Native application port, constructs the Native adapter, creates a finite provider-neutral `MemoryFabric`, and registers exactly one Native provider at an explicit revision and mode. It starts no worker, opens no store, reads no ambient configuration, and performs no handshake during construction.

Concrete adapter types remain inside this crate. Public transports, host-specific integrations, the fabric, and the provider API never import them.
